package dynamodbcopy_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uniplaces/dynamodbcopy"
	"github.com/uniplaces/dynamodbcopy/mocks"
)

const (
	srcTableName = "src-table-name"
	trgTableName = "trg-table-name"
)

func TestFetchProvisioning(t *testing.T) {
	t.Parallel()

	srcDescription := buildDefaultTableDescription(srcTableName)
	trgDescription := buildDefaultTableDescription(trgTableName)

	expectedTableDescriptions := dynamodbcopy.NewProvisioning(srcDescription, trgDescription)

	srcService := &mocks.DynamoDBService{}
	srcService.
		On("DescribeTable").
		Return(&srcDescription, nil).
		Once()

	trgService := &mocks.DynamoDBService{}
	trgService.
		On("DescribeTable").
		Return(&trgDescription, nil).
		Once()

	copyService, err := dynamodbcopy.NewDynamoDBCopy(dynamodbcopy.Config{}, srcService, trgService)
	require.Nil(t, err)

	description, err := copyService.FetchProvisioning()
	require.Nil(t, err)

	assert.Equal(t, expectedTableDescriptions, description)

	srcService.AssertExpectations(t)
	trgService.AssertExpectations(t)
}

func TestFetchProvisioning_SrcError(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("dynamo errors")

	srcService := &mocks.DynamoDBService{}
	srcService.
		On("DescribeTable").
		Return(nil, expectedError).
		Once()

	trgService := &mocks.DynamoDBService{}

	copyService, err := dynamodbcopy.NewDynamoDBCopy(dynamodbcopy.Config{}, srcService, trgService)
	require.Nil(t, err)

	_, err = copyService.FetchProvisioning()

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	srcService.AssertExpectations(t)
	trgService.AssertExpectations(t)
}

func TestFetchProvisioning_TrgError(t *testing.T) {
	t.Parallel()

	srcDescription := buildDefaultTableDescription(srcTableName)

	expectedError := errors.New("dynamo errors")

	srcService := &mocks.DynamoDBService{}
	srcService.
		On("DescribeTable").
		Return(&srcDescription, nil).
		Once()

	trgService := &mocks.DynamoDBService{}
	trgService.
		On("DescribeTable").
		Return(nil, expectedError).
		Once()

	copyService, err := dynamodbcopy.NewDynamoDBCopy(dynamodbcopy.Config{}, srcService, trgService)
	require.Nil(t, err)

	_, err = copyService.FetchProvisioning()

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	srcService.AssertExpectations(t)
	trgService.AssertExpectations(t)
}

func TestUpdateProvisioning_FetchSrcError(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("dynamo errors")

	srcService := &mocks.DynamoDBService{}
	trgService := &mocks.DynamoDBService{}

	copyService, err := dynamodbcopy.NewDynamoDBCopy(dynamodbcopy.Config{}, srcService, trgService)
	require.Nil(t, err)

	srcService.
		On("DescribeTable").
		Return(nil, expectedError).
		Once()

	_, err = copyService.UpdateProvisioning(dynamodbcopy.Provisioning{})

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	srcService.AssertExpectations(t)
	trgService.AssertExpectations(t)
}

func TestUpdateProvisioning_FetchTrgError(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("dynamo errors")

	srcDescription := buildDefaultTableDescription(srcTableName)

	srcService := &mocks.DynamoDBService{}
	trgService := &mocks.DynamoDBService{}

	copyService, err := dynamodbcopy.NewDynamoDBCopy(dynamodbcopy.Config{}, srcService, trgService)
	require.Nil(t, err)

	srcService.
		On("DescribeTable").
		Return(&srcDescription, nil).
		Once()

	trgService.
		On("DescribeTable").
		Return(nil, expectedError).
		Once()

	_, err = copyService.UpdateProvisioning(dynamodbcopy.Provisioning{})

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	srcService.AssertExpectations(t)
	trgService.AssertExpectations(t)
}

func TestUpdateProvisioning_NoUpdateNeeded(t *testing.T) {
	t.Parallel()

	srcDescription := buildDefaultTableDescription(srcTableName)
	trgDescription := buildDefaultTableDescription(trgTableName)

	srcService := &mocks.DynamoDBService{}
	trgService := &mocks.DynamoDBService{}

	copyService, err := dynamodbcopy.NewDynamoDBCopy(dynamodbcopy.Config{}, srcService, trgService)
	require.Nil(t, err)

	srcService.
		On("DescribeTable").
		Return(&srcDescription, nil).
		Once()

	trgService.
		On("DescribeTable").
		Return(&trgDescription, nil).
		Once()

	provisioning := dynamodbcopy.NewProvisioning(
		buildDefaultTableDescription(srcTableName),
		buildDefaultTableDescription(trgTableName),
	)
	updatedProvisioning, err := copyService.UpdateProvisioning(provisioning)

	require.Nil(t, err)
	assert.Equal(t, srcDescription, updatedProvisioning.Source)
	assert.Equal(t, trgDescription, updatedProvisioning.Target)

	srcService.AssertExpectations(t)
	trgService.AssertExpectations(t)
}

func TestUpdateProvisioning_SrcUpdateNeeded(t *testing.T) {
	t.Parallel()

	srcDescription := buildDefaultTableDescription(srcTableName)
	trgDescription := buildDefaultTableDescription(trgTableName)

	srcService := &mocks.DynamoDBService{}
	trgService := &mocks.DynamoDBService{}

	copyService, err := dynamodbcopy.NewDynamoDBCopy(dynamodbcopy.Config{}, srcService, trgService)
	require.Nil(t, err)

	srcService.
		On("DescribeTable").
		Return(&srcDescription, nil).
		Once()

	trgService.
		On("DescribeTable").
		Return(&trgDescription, nil).
		Once()

	srcService.
		On("UpdateCapacity", dynamodbcopy.Capacity{Read: 10, Write: 10}).
		Return(nil).
		Once()

	provisioning := dynamodbcopy.NewProvisioning(
		buildTableDescription(srcTableName, 10, 10),
		buildDefaultTableDescription(trgTableName),
	)

	updatedProvisioning, err := copyService.UpdateProvisioning(provisioning)
	require.Nil(t, err)

	assert.EqualValues(t, 10, *updatedProvisioning.Source.ProvisionedThroughput.WriteCapacityUnits)
	assert.EqualValues(t, 10, *updatedProvisioning.Source.ProvisionedThroughput.ReadCapacityUnits)
	assert.Equal(t, trgDescription, updatedProvisioning.Target)

	srcService.AssertExpectations(t)
	trgService.AssertExpectations(t)
}

func TestUpdateProvisioning_TrgUpdateNeeded(t *testing.T) {
	t.Parallel()

	srcDescription := buildDefaultTableDescription(srcTableName)
	trgDescription := buildDefaultTableDescription(trgTableName)

	srcService := &mocks.DynamoDBService{}
	trgService := &mocks.DynamoDBService{}

	copyService, err := dynamodbcopy.NewDynamoDBCopy(dynamodbcopy.Config{}, srcService, trgService)
	require.Nil(t, err)

	srcService.
		On("DescribeTable").
		Return(&srcDescription, nil).
		Once()

	trgService.
		On("DescribeTable").
		Return(&trgDescription, nil).
		Once()

	trgService.
		On("UpdateCapacity", dynamodbcopy.Capacity{Read: 10, Write: 10}).
		Return(nil).
		Once()

	provisioning := dynamodbcopy.NewProvisioning(
		buildDefaultTableDescription(srcTableName),
		buildTableDescription(trgTableName, 10, 10),
	)

	updatedProvisioning, err := copyService.UpdateProvisioning(provisioning)
	require.Nil(t, err)

	assert.EqualValues(t, 10, *updatedProvisioning.Target.ProvisionedThroughput.WriteCapacityUnits)
	assert.EqualValues(t, 10, *updatedProvisioning.Target.ProvisionedThroughput.ReadCapacityUnits)
	assert.Equal(t, srcDescription, updatedProvisioning.Source)

	srcService.AssertExpectations(t)
	trgService.AssertExpectations(t)
}

func TestUpdateProvisioning_Update(t *testing.T) {
	t.Parallel()

	srcDescription := buildDefaultTableDescription(srcTableName)
	trgDescription := buildDefaultTableDescription(trgTableName)

	srcService := &mocks.DynamoDBService{}
	trgService := &mocks.DynamoDBService{}

	copyService, err := dynamodbcopy.NewDynamoDBCopy(dynamodbcopy.Config{}, srcService, trgService)
	require.Nil(t, err)

	srcService.
		On("DescribeTable").
		Return(&srcDescription, nil).
		Once()

	srcService.
		On("UpdateCapacity", dynamodbcopy.Capacity{Read: 10, Write: 10}).
		Return(nil).
		Once()

	trgService.
		On("DescribeTable").
		Return(&trgDescription, nil).
		Once()

	trgService.
		On("UpdateCapacity", dynamodbcopy.Capacity{Read: 10, Write: 10}).
		Return(nil).
		Once()

	provisioning := dynamodbcopy.NewProvisioning(
		buildTableDescription(srcTableName, 10, 10),
		buildTableDescription(trgTableName, 10, 10),
	)

	updatedProvisioning, err := copyService.UpdateProvisioning(provisioning)
	require.Nil(t, err)

	assert.EqualValues(t, 10, *updatedProvisioning.Source.ProvisionedThroughput.WriteCapacityUnits)
	assert.EqualValues(t, 10, *updatedProvisioning.Source.ProvisionedThroughput.ReadCapacityUnits)
	assert.EqualValues(t, 10, *updatedProvisioning.Target.ProvisionedThroughput.WriteCapacityUnits)
	assert.EqualValues(t, 10, *updatedProvisioning.Target.ProvisionedThroughput.ReadCapacityUnits)

	srcService.AssertExpectations(t)
	trgService.AssertExpectations(t)
}

func buildDefaultTableDescription(table string) dynamodb.TableDescription {
	return buildTableDescription(table, 5, 5)
}

func buildTableDescription(table string, r, w int64) dynamodb.TableDescription {
	return dynamodb.TableDescription{
		TableName: aws.String(table),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughputDescription{
			ReadCapacityUnits:  aws.Int64(r),
			WriteCapacityUnits: aws.Int64(w),
		},
	}
}
