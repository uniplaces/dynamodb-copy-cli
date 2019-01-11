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

func TestBatchWrite(t *testing.T) {
	t.Parallel()

	defaultBatchInput := buildBatchWriteItemInput(10)

	firstBatchInput := buildBatchWriteItemInput(25)
	secondBatchInput := buildBatchWriteItemInput(24)

	unprocessedOuput := &dynamodb.BatchWriteItemOutput{UnprocessedItems: defaultBatchInput.RequestItems}

	expectedError := errors.New("batch write error")

	testCases := []struct {
		subTestName   string
		mocker        func(api *mocks.DynamoDBAPI)
		items         []dynamodbcopy.DynamoDBItem
		expectedError error
	}{
		{
			"Error",
			func(api *mocks.DynamoDBAPI) {
				api.On("BatchWriteItem", &defaultBatchInput).Return(nil, expectedError).Once()
			},
			getItems(defaultBatchInput),
			expectedError,
		},
		{
			"NoItems",
			func(api *mocks.DynamoDBAPI) {},
			[]dynamodbcopy.DynamoDBItem{},
			nil,
		},
		{
			"LessThanMaxBatchSize",
			func(api *mocks.DynamoDBAPI) {
				api.On("BatchWriteItem", &defaultBatchInput).Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()
			},
			getItems(defaultBatchInput),
			nil,
		},
		{
			"GreaterThanMaxBatchSize",
			func(api *mocks.DynamoDBAPI) {
				api.On("BatchWriteItem", &firstBatchInput).Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()
				api.On("BatchWriteItem", &secondBatchInput).Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()
			},
			append(
				getItems(firstBatchInput),
				getItems(secondBatchInput)...,
			),
			nil,
		},
		{
			"GreaterThanMaxBatchSizeWithError",
			func(api *mocks.DynamoDBAPI) {
				api.On("BatchWriteItem", &firstBatchInput).Return(nil, expectedError).Once()
			},
			append(
				getItems(firstBatchInput),
				getItems(secondBatchInput)...,
			),
			expectedError,
		},
		{
			"UnprocessedItems",
			func(api *mocks.DynamoDBAPI) {
				api.On("BatchWriteItem", &defaultBatchInput).Return(unprocessedOuput, nil).
					Once()

				api.On("BatchWriteItem", &defaultBatchInput).Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()
			},
			getItems(defaultBatchInput),
			nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(
			testCase.subTestName,
			func(st *testing.T) {
				api := &mocks.DynamoDBAPI{}

				testCase.mocker(api)

				service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)

				err := service.BatchWrite(testCase.items)

				assert.Equal(t, testCase.expectedError, err)

				api.AssertExpectations(st)
			},
		)
	}
}

func TestScan(t *testing.T) {
	t.Parallel()

	expectedError := errors.New("scan error")

	testCases := []struct {
		subTestName   string
		mocker        func(api *mocks.DynamoDBAPI)
		totalSegments int
		segment       int
		expectedError error
	}{
		{
			"Error",
			func(api *mocks.DynamoDBAPI) {
				api.On("ScanPages", buildScanInput(1, 0), mock.Anything).Return(expectedError)
			},
			1,
			0,
			expectedError,
		},
		{
			"TotalSegmentsError",
			func(api *mocks.DynamoDBAPI) {},
			0,
			0,
			errors.New("totalSegments has to be greater than 0"),
		},
		{
			"TotalSegmentsIsOne",
			func(api *mocks.DynamoDBAPI) {
				api.On("ScanPages", buildScanInput(1, 0), mock.Anything).Return(nil)
			},
			1,
			0,
			nil,
		},
		{
			"TotalSegmentsIsGreaterThanOne",
			func(api *mocks.DynamoDBAPI) {
				api.On("ScanPages", buildScanInput(5, 2), mock.Anything).Return(nil)
			},
			5,
			2,
			nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(
			testCase.subTestName,
			func(st *testing.T) {
				api := &mocks.DynamoDBAPI{}

				testCase.mocker(api)

				service := dynamodbcopy.NewDynamoDBService(expectedTableName, api, testSleeper)

				err := service.Scan(make(chan []dynamodbcopy.DynamoDBItem), testCase.totalSegments, testCase.segment)

				assert.Equal(t, testCase.expectedError, err)

				api.AssertExpectations(st)
			},
		)
	}
}

func buildScanInput(totalSegments, segment int64) *dynamodb.ScanInput {
	if totalSegments < 2 {
		return &dynamodb.ScanInput{
			TableName: aws.String(expectedTableName),
		}
	}

	return &dynamodb.ScanInput{
		TableName:     aws.String(expectedTableName),
		TotalSegments: aws.Int64(totalSegments),
		Segment:       aws.Int64(segment),
	}
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

func getItems(batchInput dynamodb.BatchWriteItemInput) []dynamodbcopy.DynamoDBItem {
	items := make([]dynamodbcopy.DynamoDBItem, len(batchInput.RequestItems[expectedTableName]))
	for i, writeRequest := range batchInput.RequestItems[expectedTableName] {
		items[i] = dynamodbcopy.DynamoDBItem(writeRequest.PutRequest.Item)
	}

	return items
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
