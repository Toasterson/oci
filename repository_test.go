package oci

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type OCITestSuite struct {
	suite.Suite
	fs afero.Fs
}

// Make sure that VariableThatShouldStartAtFive is set to five
// before each test
func (suite *OCITestSuite) SetupTest() {
	suite.fs = afero.NewMemMapFs()
}

// All methods that begin with "Test" are run as tests within a
// suite.
func (suite *OCITestSuite) TestCreateRepository() {
	_, err := CreateRepositoryFS(suite.fs, "/test")
	require.NoError(suite.T(), err)
}

func (suite *OCITestSuite) TestOpenRepository() {
	_, err := OpenRepositoryFS(suite.fs, "/test")
	require.Error(suite.T(), err)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestOCITestSuite(t *testing.T) {
	suite.Run(t, new(OCITestSuite))
}
