package oci

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/sirupsen/logrus"
	"github.com/ztrue/tracerr"
)

var whiteoutByte = []byte("WHITEOUT")
var whiteoutLen = int64(len(whiteoutByte))
var blacklist = []string{
	"dev/zconsole",
}

type Devino struct {
	Dev uint64
	Ino uint64
}

type TarWriter struct {
	backingWriter io.Writer
	gzipWriter    *gzip.Writer
	archiveWriter *tar.Writer
	seen          map[Devino]string
	symLinks      []*tar.Header
}

func NewTarWriter(compressor ArchiveCompressor, writer io.Writer) (tarWriter *TarWriter, err error) {
	tarWriter = &TarWriter{seen: make(map[Devino]string)}
	switch compressor {
	case ArchiveCompressorGzip:
		tarWriter.gzipWriter = gzip.NewWriter(writer)
		tarWriter.archiveWriter = tar.NewWriter(tarWriter.gzipWriter)
	case ArchiveCompressorNone:
		tarWriter.backingWriter = writer
		tarWriter.archiveWriter = tar.NewWriter(tarWriter.backingWriter)
	}
	return tarWriter, nil
}

func (tarWriter *TarWriter) Close() error {

	//Write symlinks last to avoid file does not exist errors
	for _, hdr := range tarWriter.symLinks {
		if err := tarWriter.archiveWriter.WriteHeader(hdr); err != nil {
			return tracerr.Wrap(err)
		}
	}

	if err := tarWriter.archiveWriter.Flush(); err != nil {
		return tracerr.Wrap(err)
	}
	if err := tarWriter.archiveWriter.Close(); err != nil {
		return tracerr.Wrap(err)
	}
	if tarWriter.gzipWriter != nil {
		if err := tarWriter.gzipWriter.Flush(); err != nil {
			return tracerr.Wrap(err)
		}
		if err := tarWriter.gzipWriter.Close(); err != nil {
			return tracerr.Wrap(err)
		}
	}
	return nil
}

// Add a new File into the Layer Archive
func (tarWriter *TarWriter) AddEntry(realPath string, inImagePath string, info os.FileInfo, whiteout bool) (err error) {
	if strings.HasPrefix(inImagePath, "/") {
		inImagePath = inImagePath[1:]
	}

	for _, blackListEntry := range blacklist {
		// TODO fix this dirty hack.....
		// Why do I need to jump zconsole now....
		if inImagePath == blackListEntry {
			return nil
		}
	}

	var hdr *tar.Header
	switch info.Mode() & os.ModeType {
	case os.ModeSocket, os.ModeNamedPipe, os.ModeSticky, os.ModeExclusive:
		return nil
	case os.ModeDir:
		hdr, err = tar.FileInfoHeader(info, "")
		if err != nil {
			return tracerr.Wrap(err)
		}
		//Fixup Fullpath
		hdr.Name = inImagePath

		// Force GNU Format to properly support UTF-8
		hdr.Format = tar.FormatGNU
		if err = tarWriter.archiveWriter.WriteHeader(hdr); err != nil {
			return tracerr.Wrap(err)
		}
	case os.ModeDevice, os.ModeCharDevice:
		hdr, err = tar.FileInfoHeader(info, inImagePath)
		if err != nil {
			return tracerr.Wrap(err)
		}
		hdr.Name = inImagePath

		// Force GNU Format to properly support UTF-8
		hdr.Format = tar.FormatGNU
		if err = tarWriter.archiveWriter.WriteHeader(hdr); err != nil {
			return tracerr.Wrap(err)
		}
	case os.ModeSymlink:
		//We have a Symlink thus Create it on the Target
		dstTarget, err := os.Readlink(realPath)
		if err != nil {
			return tracerr.Wrap(err)
		}
		hdr, err = tar.FileInfoHeader(info, dstTarget)
		if err != nil {
			return tracerr.Wrap(err)
		}
		hdr.Name = inImagePath

		// Force GNU Format to properly support UTF-8
		hdr.Format = tar.FormatGNU
		tarWriter.symLinks = append(tarWriter.symLinks, hdr)
	default:
		if whiteout {
			if err = tarWriter.WhiteoutFile(inImagePath); err != nil {
				return tracerr.Wrap(err)
			}

			return nil
		}

		fileObj, err := os.Open(realPath)
		if err != nil {
			fmt.Println(info.Mode() & os.ModeType)
			return tracerr.Wrap(err)
		}
		defer fileObj.Close()
		if _, err := fileObj.Read(make([]byte, 1, 1)); err != nil {
			//Workaround for non regular files and funky filesystems
			return nil
		}

		if _, err = fileObj.Seek(io.SeekStart, 0); err != nil {
			return tracerr.Wrap(err)
		}

		hdr, err = tar.FileInfoHeader(info, "")
		if err != nil {
			return tracerr.Wrap(err)
		}

		// This adds support for hardlinks
		st, ok := info.Sys().(*syscall.Stat_t)
		if ok {
			//Workaround for edge cases where conversion might fail
			// Just assume it is a file
			di := Devino{
				Dev: st.Dev,
				Ino: st.Ino,
			}

			orig, ok := tarWriter.seen[di]
			if ok {
				hdr.Typeflag = tar.TypeLink
				hdr.Linkname = orig
				hdr.Size = 0
			} else {
				tarWriter.seen[di] = inImagePath
			}
		}

		hdr.Name = inImagePath

		// Force GNU Format to properly support UTF-8
		hdr.Format = tar.FormatGNU
		if err = tarWriter.archiveWriter.WriteHeader(hdr); err != nil {
			return tracerr.Wrap(err)
		}

		if hdr.Typeflag == tar.TypeReg {
			if _, err := io.Copy(tarWriter.archiveWriter, fileObj); err != nil {
				logrus.Errorf("file: %s (%s); length: %s\n", realPath, inImagePath, humanize.Bytes(uint64(info.Size())))
				logrus.Errorf("header: length: %s\n", humanize.Bytes(uint64(hdr.Size)))
				return tracerr.Wrap(err)
			}
		}
	}

	return nil
}

func (tarWriter *TarWriter) AddTree(path, targetBasePath string) error {
	logrus.Debugf("packing directory %s as %s", path, targetBasePath)
	if err := filepath.Walk(path, func(fPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		tPath := strings.ReplaceAll(fPath, path, "")
		logrus.Debugf("Adding %s -> %s", fPath, filepath.Join(targetBasePath, tPath))
		err = tarWriter.AddEntry(fPath, tPath, info, false)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return tracerr.Wrap(err)
	}

	return nil
}

func (tarWriter *TarWriter) WhiteoutFile(name string) error {
	whName := filepath.Join(filepath.Dir(name), ".wh."+filepath.Base(name))
	hdr := tar.Header{
		Size:       whiteoutLen,
		Name:       whName,
		Format:     tar.FormatGNU,
		Uid:        0,
		Gid:        0,
		Mode:       0222,
		ModTime:    time.Now(),
		AccessTime: time.Now(),
		ChangeTime: time.Now(),
	}
	if err := tarWriter.archiveWriter.WriteHeader(&hdr); err != nil {
		return tracerr.Wrap(err)
	}
	if _, err := tarWriter.archiveWriter.Write(whiteoutByte); err != nil {
		return tracerr.Wrap(err)
	}
	return nil
}
