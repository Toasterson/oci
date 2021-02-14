package oci

import (
	"encoding/json"
	"path/filepath"

	"github.com/opencontainers/go-digest"
	specsv1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spf13/afero"
)

type DescriptorReader struct {
	backingFile afero.File
	fs          afero.Fs
}

func (d *DescriptorReader) Read(p []byte) (n int, err error) {
	return d.backingFile.Read(p)
}

func (d *DescriptorReader) Close() error {
	return d.backingFile.Close()
}

func (d *DescriptorReader) Decode(ptr interface{}) error {
	return json.NewDecoder(d).Decode(ptr)
}

func NewDescriptorReaderFsDigest(fs afero.Fs, digest digest.Digest) (*DescriptorReader, error) {
	file, err := fs.Open(filepath.Join(blobsDirectory, digest.Algorithm().String(), digest.Encoded()))
	if err != nil {
		return nil, err
	}

	return &DescriptorReader{
		fs:          fs,
		backingFile: file,
	}, nil
}

func NewDescriptorReaderFs(fs afero.Fs, descriptor specsv1.Descriptor) (*DescriptorReader, error) {
	file, err := fs.Open(filepath.Join(blobsDirectory, descriptor.Digest.Algorithm().String(), descriptor.Digest.Encoded()))
	if err != nil {
		return nil, err
	}

	return &DescriptorReader{
		fs:          fs,
		backingFile: file,
	}, nil
}
