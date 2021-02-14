package oci

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	specsv1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spf13/afero"
)

const (
	imageIndexEntrypointFileName = "index.json"
	blobsDirectory               = "blobs"
)

type ImageLayout struct {
	layout specsv1.ImageLayout
	index  specsv1.Index
	fs     afero.Fs
}

func openImageLayout(repoFs afero.Fs, name string) (*ImageLayout, error) {
	indexFile, err := repoFs.Open(filepath.Join(name, imageIndexEntrypointFileName))
	if err != nil {
		return nil, fmt.Errorf("index.json of image %s cannot be opened: %w", name, err)
	}
	defer indexFile.Close()

	img := ImageLayout{
		fs: afero.NewBasePathFs(repoFs, name),
	}

	// Check if index is the proper object
	if err := json.NewDecoder(indexFile).Decode(&img.index); err != nil {
		return nil, fmt.Errorf("index.json of image %s cannot be opened: %w", name, err)
	}

	layoutFile, err := repoFs.Open(filepath.Join(name, imageIndexEntrypointFileName))
	if err != nil {
		return nil, fmt.Errorf("%s of image %s cannot be opened: %w", specsv1.ImageLayoutFile, name, err)
	}
	defer layoutFile.Close()

	// Check if index is the proper object
	if err := json.NewDecoder(layoutFile).Decode(&img.layout); err != nil {
		return nil, fmt.Errorf("%s of image %s cannot be opened: %w", specsv1.ImageLayoutFile, name, err)
	}

	if exists, err := afero.DirExists(repoFs, filepath.Join(name, blobsDirectory)); err != nil {
		return nil, err
	} else {
		if !exists {
			return nil, fmt.Errorf("blobs directory of image %s does not exist", name)
		}
	}

	return &img, nil
}

func createImageLayout(repoFs afero.Fs, name string) (*ImageLayout, error) {
	for _, path := range []string{
		name,
		filepath.Join(name, specsv1.ImageLayoutFile),
		filepath.Join(name, imageIndexEntrypointFileName),
		filepath.Join(name, blobsDirectory),
	} {
		if exists, err := afero.Exists(repoFs, path); err != nil {
			return nil, err
		} else {
			if exists {
				return nil, fmt.Errorf("image %s already exists", name)
			}
		}
	}

	img := ImageLayout{
		layout: specsv1.ImageLayout{Version: specsv1.ImageLayoutVersion},
		index:  specsv1.Index{Versioned: specs.Versioned{SchemaVersion: 2}},
		fs:     afero.NewBasePathFs(repoFs, name),
	}

	if err := repoFs.Mkdir(name, 0755); err != nil {
		return nil, err
	}

	layoutFile, err := repoFs.Create(filepath.Join(name, specsv1.ImageLayoutFile))
	if err != nil {
		return nil, err
	}

	if err := json.NewEncoder(layoutFile).Encode(img.layout); err != nil {
		return nil, err
	}

	indexFile, err := repoFs.Create(filepath.Join(name, imageIndexEntrypointFileName))
	if err != nil {
		return nil, err
	}

	if err := json.NewEncoder(indexFile).Encode(img.index); err != nil {
		return nil, err
	}

	if err := repoFs.Mkdir(filepath.Join(name, blobsDirectory), 0755); err != nil {
		return nil, err
	}

	return &img, nil
}

func (layout *ImageLayout) OpenImage(reference string) (*Image, error) {
	panic("Implement me")
}

func (layout *ImageLayout) CreateImage(reference string) *Image {
	t := time.Now()
	return &Image{
		fs: layout.fs,
		manifest: specsv1.Manifest{
			Versioned: specs.Versioned{
				SchemaVersion: 2,
			},
			Annotations: map[string]string{
				specsv1.AnnotationRefName: reference,
			},
			Layers: make([]specsv1.Descriptor, 0),
		},
		Config: specsv1.Image{
			Created:      &t,
			Architecture: runtime.GOARCH,
			OS:           runtime.GOOS,
			History:      make([]specsv1.History, 0),
			RootFS: specsv1.RootFS{
				Type:    "rootfs",
				DiffIDs: make([]digest.Digest, 0),
			},
		},
	}
}
func (layout *ImageLayout) AddAnnotation(key, value string) {
	layout.index.Annotations[key] = value
}

func (layout *ImageLayout) SaveImage(img *Image) error {
	descr, err := img.Close()
	if err != nil {
		return err
	}

	layout.index.Manifests = append(layout.index.Manifests, descr)

	return nil
}

func (layout *ImageLayout) Close() error {
	indexFile, err := layout.fs.Create(imageIndexEntrypointFileName)
	if err != nil {
		return err
	}

	return json.NewEncoder(indexFile).Encode(layout.index)
}
