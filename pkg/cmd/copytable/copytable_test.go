package copytable_test

import (
	"errors"
	"testing"

	"github.com/magiconair/properties/assert"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/uniplaces/dynamodbcopy/mocks"
	"github.com/uniplaces/dynamodbcopy/pkg/cmd/copytable"
)

func TestRun_FetchProvisioningError(t *testing.T) {
	t.Parallel()

	service := &mocks.Copier{}

	expectedError := errors.New("error")
	service.
		On("FetchProvisioning").
		Return(nil, expectedError).
		Once()

	err := copytable.RunCopyTable(service)

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	service.AssertExpectations(t)
}

func TestRun_UpdateProvisioningError(t *testing.T) {
	t.Parallel()

	service := &mocks.Copier{}
	provisioning := &mocks.TableProvisioner{}

	expectedError := errors.New("error")
	service.
		On("FetchProvisioning").
		Return(provisioning, nil).
		Once()

	provisioning.
		On("NeedsUpdate").
		Return(true).
		Once()

	service.
		On("UpdateProvisioning", provisioning).
		Return(expectedError).
		Once()

	err := copytable.RunCopyTable(service)

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	service.AssertExpectations(t)
	provisioning.AssertExpectations(t)
}

func TestRun_CopyError(t *testing.T) {
	t.Parallel()

	service := &mocks.Copier{}
	provisioning := &mocks.TableProvisioner{}

	expectedError := errors.New("error")
	service.
		On("FetchProvisioning").
		Return(provisioning, nil).
		Once()

	provisioning.
		On("NeedsUpdate").
		Return(true).
		Once()

	service.
		On("UpdateProvisioning", provisioning).
		Return(nil).
		Once()

	service.
		On("Copy").
		Return(expectedError).
		Once()

	err := copytable.RunCopyTable(service)

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	service.AssertExpectations(t)
	provisioning.AssertExpectations(t)
}

func TestRun_UpdateProvisioningEndError(t *testing.T) {
	t.Parallel()

	service := &mocks.Copier{}
	provisioning := &mocks.TableProvisioner{}

	expectedError := errors.New("error")

	service.
		On("FetchProvisioning").
		Return(provisioning, nil).
		Once()

	provisioning.
		On("NeedsUpdate").
		Return(true).
		Once()

	service.
		On("UpdateProvisioning", provisioning).
		Return(nil).
		Once()

	service.
		On("Copy").
		Return(nil).
		Once()

	service.
		On("UpdateProvisioning", provisioning).
		Return(expectedError).
		Once()

	err := copytable.RunCopyTable(service)

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	service.AssertExpectations(t)
	provisioning.AssertExpectations(t)
}

func TestRun_CopyWithProvisionUpdate(t *testing.T) {
	t.Parallel()

	service := &mocks.Copier{}
	provisioning := &mocks.TableProvisioner{}

	service.
		On("FetchProvisioning").
		Return(provisioning, nil).
		Once()

	provisioning.
		On("NeedsUpdate").
		Return(true).
		Once()

	service.
		On("UpdateProvisioning", provisioning).
		Return(nil).
		Twice()

	service.
		On("Copy").
		Return(nil).
		Once()

	err := copytable.RunCopyTable(service)

	require.Nil(t, err)

	service.AssertExpectations(t)
	provisioning.AssertExpectations(t)
}

func TestRun_CopyNoProvisionUpdate(t *testing.T) {
	t.Parallel()

	service := &mocks.Copier{}
	provisioning := &mocks.TableProvisioner{}

	service.
		On("FetchProvisioning").
		Return(provisioning, nil).
		Once()

	provisioning.
		On("NeedsUpdate").
		Return(false).
		Once()

	service.
		On("Copy").
		Return(nil).
		Once()

	err := copytable.RunCopyTable(service)

	require.Nil(t, err)

	service.AssertExpectations(t)
	provisioning.AssertExpectations(t)
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
