package oci

import (
	"archive/tar"
	"strings"

	"github.com/opencontainers/go-digest"
	specsv1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spf13/afero"
)

type LayerReader struct {
	digest        digest.Digest
	archiveReader *TarReader
}

func NewLayerReader(fs afero.Fs, layer specsv1.Descriptor) (*LayerReader, error) {
	l := &LayerReader{
		digest: layer.Digest,
	}

	var compressor ArchiveCompressor
	if strings.Contains(layer.MediaType, "gzip") || strings.Contains(layer.MediaType, "gz") {
		compressor = ArchiveCompressorGzip
	} else {
		compressor = ArchiveCompressorNone
	}

	var err error
	l.archiveReader, err = NewTarReader(compressor, afero.NewBasePathFs(fs, l.digest.Algorithm().String()), l.digest.Encoded())
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *LayerReader) ExtractTreeInto(targetFs afero.Fs) error {
	return l.archiveReader.ExtractTreeInto(targetFs)
}

func (l *LayerReader) Next() (*tar.Header, error) {
	return l.archiveReader.Next()
}

func (l *LayerReader) Close() error {
	return l.archiveReader.Close()
}
