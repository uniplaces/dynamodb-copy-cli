package dynamodbcopy_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/uniplaces/dynamodbcopy"
	"github.com/uniplaces/dynamodbcopy/mocks"
)

const (
	expectedTableName = "test-table-name"
)

func TestDescribeTable(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	expectedTableDescription := buildDescribeTableOutput(expectedTableName, dynamodb.TableStatusActive)

	api.
		On("DescribeTable", mock.AnythingOfType("*dynamodb.DescribeTableInput")).
		Return(expectedTableDescription, nil).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)

	description, err := service.DescribeTable()
	require.Nil(t, err)

	assert.Equal(t, expectedTableDescription.Table, description)

	api.AssertExpectations(t)
}

func TestDescribeTable_Error(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	expectedError := errors.New("error")

	api.
		On("DescribeTable", mock.AnythingOfType("*dynamodb.DescribeTableInput")).
		Return(nil, expectedError).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)

	_, err := service.DescribeTable()
	require.NotNil(t, err)

	assert.Equal(t, expectedError, err)

	api.AssertExpectations(t)
}

func TestUpdateCapacity_ZeroError(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)
	err := service.UpdateCapacity(dynamodbcopy.Capacity{Read: 0, Write: 10})

	require.NotNil(t, err)

	api.AssertExpectations(t)
}

func TestUpdateCapacity_Error(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	expectedError := errors.New("error")

	api.
		On("UpdateTable", mock.AnythingOfType("*dynamodb.UpdateTableInput")).
		Return(nil, expectedError).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)
	err := service.UpdateCapacity(dynamodbcopy.Capacity{Read: 10, Write: 10})

	require.NotNil(t, err)
	assert.Equal(t, expectedError, err)

	api.AssertExpectations(t)
}

func TestUpdateCapacity(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	api.
		On("UpdateTable", mock.AnythingOfType("*dynamodb.UpdateTableInput")).
		Return(&dynamodb.UpdateTableOutput{}, nil).
		Once()

	api.
		On("DescribeTable", mock.AnythingOfType("*dynamodb.DescribeTableInput")).
		Return(buildDescribeTableOutput(expectedTableName, dynamodb.TableStatusActive), nil).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)
	err := service.UpdateCapacity(dynamodbcopy.Capacity{Read: 10, Write: 10})

	require.Nil(t, err)

	api.AssertExpectations(t)
}

func TestWaitForReadyTable_Error(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	expectedError := errors.New("error")
	api.
		On("DescribeTable", mock.AnythingOfType("*dynamodb.DescribeTableInput")).
		Return(nil, expectedError).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)
	err := service.WaitForReadyTable()

	require.NotNil(t, err)

	api.AssertExpectations(t)
}

func TestWaitForReadyTable_OnFirstAttempt(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	called := 0
	sleeperFn := func(elapsedMilliseconds int) int {
		called++

		return called
	}

	api.
		On("DescribeTable", mock.AnythingOfType("*dynamodb.DescribeTableInput")).
		Return(buildDescribeTableOutput(expectedTableName, dynamodb.TableStatusActive), nil).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, sleeperFn)
	err := service.WaitForReadyTable()

	require.Nil(t, err)
	assert.Equal(t, 0, called)

	api.AssertExpectations(t)
}

func TestWaitForReadyTable_OnMultipleAttempts(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	attempts := 4

	called := 0
	sleeperFn := func(elapsedMilliseconds int) int {
		called++

		return elapsedMilliseconds
	}

	api.
		On("DescribeTable", mock.AnythingOfType("*dynamodb.DescribeTableInput")).
		Return(buildDescribeTableOutput(expectedTableName, dynamodb.TableStatusCreating), nil).
		Times(attempts)

	api.
		On("DescribeTable", mock.AnythingOfType("*dynamodb.DescribeTableInput")).
		Return(buildDescribeTableOutput(expectedTableName, dynamodb.TableStatusActive), nil).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, sleeperFn)
	err := service.WaitForReadyTable()

	require.Nil(t, err)
	assert.Equal(t, attempts, called)

	api.AssertExpectations(t)
}

func TestBatchWrite_Error(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	expectedError := errors.New("error")
	batchInput := buildBatchWriteItemInput(10)

	api.
		On("BatchWriteItem", &batchInput).
		Return(nil, expectedError).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)

	err := service.BatchWrite(batchInput.RequestItems[expectedTableName])

	require.NotNil(t, err)

	api.AssertExpectations(t)
}

func TestBatchWrite_NoItems(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	batchInput := buildBatchWriteItemInput(0)
	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)
	err := service.BatchWrite(batchInput.RequestItems[expectedTableName])

	require.Nil(t, err)

	api.AssertExpectations(t)
}

func TestBatchWrite_LessThanMaxBatchSize(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	batchInput := buildBatchWriteItemInput(24)

	api.
		On("BatchWriteItem", &batchInput).
		Return(&dynamodb.BatchWriteItemOutput{}, nil).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)

	err := service.BatchWrite(batchInput.RequestItems[expectedTableName])

	require.Nil(t, err)

	api.AssertExpectations(t)
}

func TestBatchWrite_GreaterThanMaxBatchSize(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	firstBatchInput := buildBatchWriteItemInput(25)
	secondBatchInput := buildBatchWriteItemInput(24)

	api.
		On("BatchWriteItem", &firstBatchInput).
		Return(&dynamodb.BatchWriteItemOutput{}, nil).
		Once()

	api.
		On("BatchWriteItem", &secondBatchInput).
		Return(&dynamodb.BatchWriteItemOutput{}, nil).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)

	requests := append(
		firstBatchInput.RequestItems[expectedTableName],
		secondBatchInput.RequestItems[expectedTableName]...,
	)
	err := service.BatchWrite(requests)

	require.Nil(t, err)

	api.AssertExpectations(t)
}

func TestBatchWrite_UnprocessedItems(t *testing.T) {
	api := &mocks.DynamoDBAPI{}

	batchInput := buildBatchWriteItemInput(25)

	api.
		On("BatchWriteItem", &batchInput).
		Return(&dynamodb.BatchWriteItemOutput{UnprocessedItems: batchInput.RequestItems}, nil).
		Once()

	api.
		On("BatchWriteItem", &batchInput).
		Return(&dynamodb.BatchWriteItemOutput{}, nil).
		Once()

	service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)

	err := service.BatchWrite(batchInput.RequestItems[expectedTableName])

	require.Nil(t, err)

	api.AssertExpectations(t)
}

func buildBatchWriteItemInput(itemCount int) dynamodb.BatchWriteItemInput {
	items := map[string][]*dynamodb.WriteRequest{}

	requests := make([]*dynamodb.WriteRequest, itemCount)
	for i := 0; i < itemCount; i++ {
		requests[i] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String(fmt.Sprintf("%d", i)),
					},
				},
			},
		}
	}

	items[expectedTableName] = requests

	return dynamodb.BatchWriteItemInput{
		RequestItems: items,
	}
}

func buildDescribeTableOutput(tableName, status string) *dynamodb.DescribeTableOutput {
	return &dynamodb.DescribeTableOutput{
		Table: &dynamodb.TableDescription{
			TableName:   aws.String(tableName),
			TableStatus: aws.String(status),
		},
	}
}

func testSleeper(ms int) int {
	return ms // skip
}
