package oci

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (suite *OCITestSuite) TestCreateImage() {
	testImgName := "testing"
	repo, err := CreateRepositoryFS(suite.fs, "test")
	require.NoError(suite.T(), err)
	_, err = repo.CreateImageLayout(testImgName)
	require.NoError(suite.T(), err)
	_, err = repo.OpenImageLayout(testImgName)
	require.NoError(suite.T(), err)
	_, err = repo.CreateImageLayout(testImgName)
	if !assert.Error(suite.T(), err) {
		suite.T().Fatalf("subsequent calls should return an error")
	}
	assert.True(suite.T(), repo.HasImageLayout(testImgName))
	assert.True(suite.T(), repo.IsImageLayoutConsistent(testImgName))
}

func (suite *OCITestSuite) TestOpenImage() {
	testImgName := "testing"
	repo, err := CreateRepositoryFS(suite.fs, "test")
	require.NoError(suite.T(), err)
	_, err = repo.CreateImageLayout(testImgName)
	require.NoError(suite.T(), err)
	_, err = repo.OpenImageLayout(testImgName)
	assert.NoError(suite.T(), err)
	assert.True(suite.T(), repo.HasImageLayout(testImgName))
	assert.True(suite.T(), repo.IsImageLayoutConsistent(testImgName))
}
