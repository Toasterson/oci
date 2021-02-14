package oci

import (
	"errors"
	"path/filepath"

	specsv1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spf13/afero"
)

var (
	ErrRepoExists   = errors.New("repository exists")
	ErrNoRepository = errors.New("not a repository")
)

type Repository struct {
	fs afero.Fs
}

// Helper Method which creates an empty repository if it does not exists on path
// returns an error if a repository exists on path
func CreateRepository(path string) (*Repository, error) {
	return CreateRepositoryFS(afero.NewOsFs(), path)
}

func CreateRepositoryFS(fs afero.Fs, path string) (*Repository, error) {
	if IsRepository(path) {
		return nil, ErrRepoExists
	}

	if err := fs.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	return &Repository{fs: afero.NewBasePathFs(fs, path)}, nil
}

// Helper function which opens an existing repository
// returns an error if no repository exists on path
func OpenRepository(path string) (*Repository, error) {
	return OpenRepositoryFS(afero.NewOsFs(), path)
}

func OpenRepositoryFS(fs afero.Fs, path string) (*Repository, error) {
	if !IsRepository(path) {
		return nil, ErrNoRepository
	}

	return &Repository{fs: afero.NewBasePathFs(fs, path)}, nil
}

// Helper which checks if a OCI repository resides in the given path
func IsRepository(path string) bool {
	return IsRepositoryFs(afero.NewOsFs(), path)
}

func IsRepositoryFs(fs afero.Fs, path string) bool {
	exists, err := afero.DirExists(fs, path)
	if err != nil {
		return false
	}
	return exists
}

func (r *Repository) OpenImageLayout(name string) (*ImageLayout, error) {
	return openImageLayout(r.fs, name)
}

func (r *Repository) CreateImageLayout(name string) (*ImageLayout, error) {
	return createImageLayout(r.fs, name)
}

func (r *Repository) HasImageLayout(name string) bool {
	if exists, err := afero.Exists(r.fs, filepath.Join(name, specsv1.ImageLayoutFile)); err != nil {
		return false
	} else {
		if !exists {
			return false
		}
	}

	return true
}

func (r Repository) IsImageLayoutConsistent(name string) bool {
	_, err := openImageLayout(r.fs, name)
	if err != nil {
		return false
	}
	return true
}
