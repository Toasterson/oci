package oci

import (
	"os"

	"github.com/opencontainers/go-digest"
	specsv1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spf13/afero"
)

const LayerTempfilePrefix = ".oci.newlayer"

type LayerWriter struct {
	algorithm     digest.Algorithm
	descF         *DescriptorWriter
	archiveWriter *TarWriter
	fs            afero.Fs
}

func (img *Image) NewLayerWriter(algorithm digest.Algorithm) (l *LayerWriter, err error) {
	l = &LayerWriter{
		fs: img.fs,
	}

	descWr, err := NewDescriptorWriterFs(img.fs, "/", specsv1.MediaTypeImageLayerGzip, algorithm, nil)
	if err != nil {
		return nil, err
	}

	l.descF = descWr

	l.archiveWriter, err = NewTarWriter(ArchiveCompressorGzip, descWr)
	return l, nil
}

func (l *LayerWriter) Close() (specsv1.Descriptor, error) {
	if err := l.archiveWriter.Close(); err != nil {
		return specsv1.Descriptor{}, err
	}

	return l.descF.Close()
}

// Add a new File into the Layer Archive
func (l *LayerWriter) AddEntry(realPath string, inImagePath string, info os.FileInfo, whiteout bool) (err error) {
	return l.archiveWriter.AddEntry(realPath, inImagePath, info, whiteout)
}
