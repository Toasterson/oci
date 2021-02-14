package oci

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/afero"
)

var minTarDate = time.Date(1910, 1, 1, 1, 1, 1, 1, time.Local)
var maxTarDate = time.Now().Add(7000 * time.Second)

type ArchiveCompressor int

const (
	ArchiveCompressorNone ArchiveCompressor = iota
	ArchiveCompressorGzip
)

type TarReader struct {
	backingFile   afero.File
	backingReader io.Reader
	archiveReader *tar.Reader
}

func NewTarReader(compressor ArchiveCompressor, fs afero.Fs, fName string) (tarReader *TarReader, err error) {
	tarReader = &TarReader{}
	f, err := fs.Open(fName)
	if err != nil {
		return nil, err
	}
	tarReader.backingFile = f
	switch compressor {
	case ArchiveCompressorNone:
		tarReader.backingReader = f
	case ArchiveCompressorGzip:
		tarReader.backingReader, err = gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
	}
	tarReader.archiveReader = tar.NewReader(tarReader.backingReader)
	return tarReader, nil
}

func (tarReader *TarReader) ExtractTreeInto(targetFs afero.Fs) error {
	symLinkList := make([]tar.Header, 0)
	for {
		th, err := tarReader.archiveReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if strings.Contains(th.Name, ".wh.") {
			if err := targetFs.Remove(strings.Replace(th.Name, ".wh.", "", -1)); err != nil && !os.IsNotExist(err) {
				return err
			}

			if _, err = io.Copy(ioutil.Discard, tarReader.archiveReader); err != nil {
				return err
			}

		} else {
			//fileOrDirPath := filepath.Join(dir, th.Name)
			switch th.Typeflag {
			case tar.TypeDir:
				//logrus.Tracef("Extracting Directory %s", fileOrDirPath)
				if err := unpackDir(th, targetFs); err != nil {
					return err
				}
			case tar.TypeSymlink:
				//Defer symlink creation to later to avoid file not exist errors
				//logrus.Tracef("Saving Symlink %s for later extraction", fileOrDirPath)
				symLinkList = append(symLinkList, *th)
			case tar.TypeLink:
				// Links must be created relative to dir in order to find a file that already exists
				// Hardlinks are resolved at link time rather than symlinks which are resolved at runtime
				//logrus.Tracef("Extracting Hardlink %s", fileOrDirPath)
				if targetLinker, ok := targetFs.(afero.Linker); ok {
					if err := targetLinker.SymlinkIfPossible(th.Linkname, th.Name); err != nil {
						return err
					}
				}
			case tar.TypeReg:
				//logrus.Tracef("Extracting File %s", fileOrDirPath)
				if err := unpackFile(th, tarReader.archiveReader, targetFs); err != nil {
					return err
				}
			case tar.TypeChar, tar.TypeBlock:
				//TODO Implement Block devices
				//TODO Implement Char devices
				continue
			}
		}
	}

	for _, th := range symLinkList {
		//logrus.Tracef("Extracting Symlink %s", path)
		if targetLinker, ok := targetFs.(afero.Linker); ok {
			if err := targetLinker.SymlinkIfPossible(th.Linkname, th.Name); err != nil {
				if os.IsExist(err) {
					// If the file exists recreate it
					// TODO do some unification and rerun functionality anonymous function +defer ???
					if err := targetFs.Remove(th.Name); err != nil {
						if os.IsNotExist(err) {
							continue
						}
						return err
					}
					if err := targetLinker.SymlinkIfPossible(th.Linkname, th.Name); err != nil {
						if os.IsExist(err) {
							continue
						}
						return err
					}
					continue
				}
				return err
			}
		}
	}
	return nil
}

func (tarReader *TarReader) Next() (*tar.Header, error) {
	return tarReader.archiveReader.Next()
}

func (tarReader *TarReader) Read(p []byte) (n int, err error) {
	return tarReader.archiveReader.Read(p)
}

func (tarReader *TarReader) Close() error {
	if err := tarReader.backingFile.Close(); err != nil {
		return err
	}
	return nil
}

func unpackDir(th *tar.Header, targetFs afero.Fs) error {
	info := th.FileInfo()
	if err := targetFs.Mkdir(th.Name, info.Mode()); err != nil && !os.IsExist(err) {
		return err
	}
	if err := targetFs.Chown(th.Name, th.Uid, th.Gid); err != nil {
		return err
	}

	if err := safeChtimes(targetFs, th.Name, th.AccessTime, th.ModTime); err != nil {
		return err
	}
	return nil
}

func unpackFile(th *tar.Header, tr io.Reader, targetFs afero.Fs) error {
	f, err := targetFs.Create(th.Name)
	if err != nil {
		return err
	}
	defer f.Close()
	if written, err := io.Copy(f, tr); err != nil {
		if strings.Contains(err.Error(), "EOF") {
			fmt.Printf("file: %s; length expected: %s; length written %s\n", th.Name, humanize.Bytes(uint64(th.Size)), humanize.Bytes(uint64(written)))
		}
		return err
	}
	if err := targetFs.Chmod(th.Name, th.FileInfo().Mode()); err != nil {
		return err
	}
	if err := targetFs.Chown(th.Name, th.Uid, th.Gid); err != nil {
		return err
	}
	if err := safeChtimes(targetFs, th.Name, th.AccessTime, th.ModTime); err != nil {
		return err
	}
	return nil
}

func safeChtimes(targetFs afero.Fs, fielName string, atime, mtime time.Time) error {
	if mtime.Before(minTarDate) || mtime.After(maxTarDate) {
		mtime = time.Now()
	}

	if atime.Before(minTarDate) || atime.After(maxTarDate) {
		atime = mtime
	}

	if err := targetFs.Chtimes(fielName, atime, mtime); err != nil {
		return err
	}

	return nil
}
