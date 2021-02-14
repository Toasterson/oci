package oci

import (
	"encoding/json"
	"io"
	"path/filepath"

	"github.com/opencontainers/go-digest"
	specsv1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spf13/afero"
)

type DescriptorWriter struct {
	backingFile afero.File
	writer      io.Writer
	digester    digest.Digester
	descriptor  specsv1.Descriptor
	fs          afero.Fs
}

func (d *DescriptorWriter) Close() (specsv1.Descriptor, error) {
	if err := d.backingFile.Close(); err != nil {
		return specsv1.Descriptor{}, err
	}

	d.descriptor.Digest = d.digester.Digest()

	if err := d.fs.Rename(d.backingFile.Name(), filepath.Join(blobsDirectory, d.descriptor.Digest.Algorithm().String(), d.descriptor.Digest.Encoded())); err != nil {
		return specsv1.Descriptor{}, err
	}

	return d.descriptor, nil
}

func (d *DescriptorWriter) Write(p []byte) (n int, err error) {
	written, err := d.writer.Write(p)
	if err != nil {
		return written, err
	}
	d.descriptor.Size += int64(written)

	return written, nil
}

func (d *DescriptorWriter) Encode(data interface{}) error {
	return json.NewEncoder(d).Encode(data)
}

func NewDescriptorWriterFs(fs afero.Fs, path string, mediaType string, alg digest.Algorithm, plat *specsv1.Platform) (*DescriptorWriter, error) {
	backingFile, err := afero.TempFile(fs, path, ".tmp.*")
	if err != nil {
		return nil, err
	}

	if !alg.Available() {
		alg = digest.Canonical
	}

	desc := &DescriptorWriter{
		backingFile: backingFile,
		digester:    alg.Digester(),
		descriptor: specsv1.Descriptor{
			MediaType: mediaType,
			Platform:  plat,
		},
	}

	desc.writer = io.MultiWriter(desc.backingFile, desc.digester.Hash())

	return desc, nil
}
