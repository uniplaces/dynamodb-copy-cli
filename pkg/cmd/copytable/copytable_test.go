package copytable_test

import (
	"errors"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uniplaces/dynamodbcopy"
	"github.com/uniplaces/dynamodbcopy/mocks"
	"github.com/uniplaces/dynamodbcopy/pkg/cmd/copytable"
)

func TestRunCopyTable_FetchProvisioningError(t *testing.T) {
	t.Parallel()

	service := &mocks.Copier{}

	expectedError := errors.New("error")
	service.
		On("FetchProvisioning").
		Return(dynamodbcopy.Provisioning{}, expectedError).
		Once()

	err := copytable.RunCopyTable(service)

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	service.AssertExpectations(t)
}

func TestRunCopyTable_UpdateProvisioningError(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("error")
	provisioning := dynamodbcopy.Provisioning{}

	service := &mocks.Copier{}
	service.
		On("FetchProvisioning").
		Return(provisioning, nil).
		Once()

	service.
		On("UpdateProvisioning", provisioning).
		Return(dynamodbcopy.Provisioning{}, expectedError).
		Once()

	err := copytable.RunCopyTable(service)

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	service.AssertExpectations(t)
}

func TestRunCopyTable_CopyError(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("error")
	provisioning := dynamodbcopy.Provisioning{}

	service := &mocks.Copier{}
	service.
		On("FetchProvisioning").
		Return(provisioning, nil).
		Once()

	service.
		On("UpdateProvisioning", provisioning).
		Return(dynamodbcopy.Provisioning{}, nil).
		Once()

	service.
		On("Copy").
		Return(expectedError).
		Once()

	err := copytable.RunCopyTable(service)

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	service.AssertExpectations(t)
}

func TestRunCopyTable_RestoreProvisioningError(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("error")
	provisioning := dynamodbcopy.Provisioning{}

	service := &mocks.Copier{}
	service.
		On("FetchProvisioning").
		Return(provisioning, nil).
		Once()

	service.
		On("UpdateProvisioning", provisioning).
		Return(dynamodbcopy.Provisioning{}, nil).
		Once()

	service.
		On("Copy").
		Return(nil).
		Once()

	service.
		On("UpdateProvisioning", provisioning).
		Return(dynamodbcopy.Provisioning{}, expectedError).
		Once()

	err := copytable.RunCopyTable(service)

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	service.AssertExpectations(t)
}

func TestRunCopyTable(t *testing.T) {
	t.Parallel()

	provisioning := dynamodbcopy.Provisioning{}

	service := &mocks.Copier{}
	service.
		On("FetchProvisioning").
		Return(provisioning, nil).
		Once()

	service.
		On("UpdateProvisioning", provisioning).
		Return(dynamodbcopy.Provisioning{}, nil).
		Once()

	service.
		On("Copy").
		Return(nil).
		Once()

	service.
		On("UpdateProvisioning", provisioning).
		Return(dynamodbcopy.Provisioning{}, nil).
		Once()

	err := copytable.RunCopyTable(service)

	require.Nil(t, err)

	service.AssertExpectations(t)
}

func TestSetAndBindFlags_Default(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{}
	config := viper.New()

	err := copytable.SetAndBindFlags(cmd.Flags(), config)

	require.Nil(t, err)

	assert.Equal(t, 4, len(config.AllSettings()))
	assert.Equal(t, "", config.GetString("source-profile"))
	assert.Equal(t, "", config.GetString("target-profile"))
	assert.Equal(t, 0, config.GetInt("read-units"))
	assert.Equal(t, 0, config.GetInt("write-units"))
}
