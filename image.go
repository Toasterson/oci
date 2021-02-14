package oci

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"github.com/opencontainers/go-digest"
	specsv1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spf13/afero"
)

type Image struct {
	manifest specsv1.Manifest
	Config   specsv1.Image
	fs       afero.Fs
}

func (img *Image) AddAnnotation(key, value string) {
	img.manifest.Annotations[key] = value
}

func (img *Image) AddMetadata(label, mediaType string, data interface{}) error {
	if label == "" {
		panic(fmt.Errorf("programmer error label for metadata not set"))
	}

	descF, err := NewDescriptorWriterFs(img.fs, "/", mediaType, digest.Canonical, nil)
	if err != nil {
		return err
	}

	if err = json.NewEncoder(descF).Encode(data); err != nil {
		return err
	}

	descriptor, err := descF.Close()
	if err != nil {
		return err
	}

	img.manifest.Annotations[label] = descriptor.Digest.String()

	return nil
}

// Note this function assumes that you have downloaded the additional blobs from the registry
func (img *Image) GetMetadata(manifestDigest digest.Digest, label string, targetDataPtr interface{}) error {
	if label == "" {
		panic(fmt.Errorf("programmer error label for metadata not set: cannot read empty string as metadata"))
	}

	rd, err := NewDescriptorReaderFsDigest(img.fs, digest.Digest(img.manifest.Annotations[label]))
	if err != nil {
		return err
	}

	return rd.Decode(targetDataPtr)
}

func (img *Image) SaveConfig(alg digest.Algorithm) error {
	descWr, err := NewDescriptorWriterFs(img.fs, "/", specsv1.MediaTypeImageConfig, alg, &specsv1.Platform{
		OS:           runtime.GOOS,
		Architecture: runtime.GOARCH,
	})
	if err != nil {
		return err
	}

	err = descWr.Encode(img.Config)
	if err != nil {
		return err
	}

	descr, err := descWr.Close()
	if err != nil {
		return err
	}

	img.manifest.Config = descr

	return nil
}

func (img *Image) AddLayerDescriptors(l []specsv1.Descriptor) {
	img.manifest.Layers = append(l, img.manifest.Layers...)
	for _, descr := range l {
		img.Config.RootFS.DiffIDs = append(img.Config.RootFS.DiffIDs, descr.Digest)
	}
}

func (img *Image) AddLayerFile(origPath, mediaType string, h specsv1.History) error {
	f, err := os.Open(origPath)
	if err != nil {
		return err
	}
	defer f.Close()

	descWr, err := NewDescriptorWriterFs(img.fs, "/", mediaType, digest.Canonical, nil)
	if err != nil {
		return err
	}

	_, err = io.Copy(descWr, f)
	if err != nil {
		return err
	}
	descr, err := descWr.Close()
	if err != nil {
		return err
	}

	img.manifest.Layers = append(img.manifest.Layers, descr)
	img.Config.RootFS.DiffIDs = append(img.Config.RootFS.DiffIDs, descr.Digest)
	img.Config.History = append(img.Config.History, h)

	return nil
}

func (img *Image) AddTree(rPath string, h specsv1.History) error {
	layer, err := img.NewLayerWriter(digest.Canonical)
	if err != nil {
		return err
	}

	err = filepath.Walk(rPath, func(path string, info os.FileInfo, err error) error {
		//Ignore /
		if path == rPath {
			return nil
		}
		inImagePath := strings.Replace(path, rPath+"/", "", -1)
		return layer.AddEntry(path, inImagePath, info, false)
	})

	if err != nil {
		return err
	}

	descr, err := layer.Close()
	if err != nil {
		return err
	}

	img.manifest.Layers = append(img.manifest.Layers, descr)
	img.Config.RootFS.DiffIDs = append(img.Config.RootFS.DiffIDs, descr.Digest)
	img.Config.History = append(img.Config.History, h)

	return nil
}

func (img *Image) AddDiff(layerFs1 afero.Fs, layerFs2 afero.Fs, layer1RootPath, layer2RootPath string, h specsv1.History) error {
	layer, err := img.NewLayerWriter(digest.Canonical)
	if err != nil {
		return err
	}

	err = afero.Walk(layerFs1, layer1RootPath, func(path string, info os.FileInfo, err error) error {
		//Do nothing with the root directory of the layer
		inImagePath := strings.Replace(path, layer1RootPath+"/", "", -1)
		if path == inImagePath {
			return nil
		}

		//First check if file has been deleted
		l2Path := strings.Replace(path, layer1RootPath, layer2RootPath, -1)
		newstat, err := os.Lstat(l2Path)
		if os.IsNotExist(err) {
			//This means we whiteout the file in the new Layer
			return layer.AddEntry(path, inImagePath, info, true)
		}

		//Check if we have a difference
		switch info.Mode() & os.ModeType {
		case os.ModeSocket, os.ModeNamedPipe, os.ModeSticky, os.ModeExclusive:
			return nil
		case os.ModeSymlink:
			//We have a Symlink thus Create it on the Target
			l1dstTarget, _ := os.Readlink(path)
			l2dstTarget, _ := os.Readlink(l2Path)
			if l2dstTarget != l1dstTarget {
				if err = layer.AddEntry(path, inImagePath, info, false); err != nil {
					return err
				}
			}
		default:
			l1md := info.Mode() & os.ModePerm
			l2md := newstat.Mode() & os.ModePerm
			l1Stat := info.Sys().(*syscall.Stat_t)
			l2Stat := info.Sys().(*syscall.Stat_t)
			if l1md != l2md || l1Stat.Uid != l2Stat.Uid || l1Stat.Gid != l2Stat.Gid || info.Size() != newstat.Size() {
				if err = layer.AddEntry(path, inImagePath, info, false); err != nil {
					return err
				}
			}

		}
		return nil
	})

	if err != nil {
		return err
	}

	// Check for new Files only
	err = afero.Walk(layerFs2, layer2RootPath, func(path string, info os.FileInfo, err error) error {
		//Do nothing with the root directory of the layer
		inImagePath := strings.Replace(path, layer2RootPath+"/", "", -1)
		if path == inImagePath {
			return nil
		}

		l1Path := strings.Replace(path, layer2RootPath, layer1RootPath, -1)
		_, err = os.Lstat(l1Path)
		//Check if the file does not exist in the previous layer
		if os.IsNotExist(err) {
			// Add new file to the layer
			if err = layer.AddEntry(path, inImagePath, info, false); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	descr, err := layer.Close()
	if err != nil {
		return err
	}
	img.manifest.Layers = append(img.manifest.Layers, descr)
	img.Config.RootFS.DiffIDs = append(img.Config.RootFS.DiffIDs, descr.Digest)
	img.Config.History = append(img.Config.History, h)

	return nil
}

func (img *Image) ExtractInto(targetFs afero.Fs, rootPath string) error {
	if rootPath != "" && rootPath != "/" {
		targetFs = afero.NewBasePathFs(targetFs, rootPath)
	}

	for _, layerDescr := range img.manifest.Layers {
		layerReader, err := NewLayerReader(img.fs, layerDescr)
		if err != nil {
			return err
		}
		err = layerReader.ExtractTreeInto(targetFs)
		if err != nil {
			return err
		}
		err = layerReader.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (img *Image) Close() (specsv1.Descriptor, error) {

	// Save image configuration to disk
	if err := img.SaveConfig(digest.Canonical); err != nil {
		return specsv1.Descriptor{}, err
	}

	// Save manifest to disk
	descWr, err := NewDescriptorWriterFs(img.fs, "/", specsv1.MediaTypeImageManifest, digest.Canonical, &specsv1.Platform{
		OS:           runtime.GOOS,
		Architecture: runtime.GOARCH,
	})
	if err != nil {
		return specsv1.Descriptor{}, err
	}

	if err = descWr.Encode(img.manifest); err != nil {
		return specsv1.Descriptor{}, err
	}

	descr, err := descWr.Close()
	if err != nil {
		return specsv1.Descriptor{}, err
	}

	if len(img.manifest.Annotations) > 0 {
		descr.Annotations = img.manifest.Annotations
	}

	return descr, nil
}
