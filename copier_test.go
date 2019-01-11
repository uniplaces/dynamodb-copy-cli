package dynamodbcopy_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/uniplaces/dynamodbcopy"
	"github.com/uniplaces/dynamodbcopy/mocks"
)

func TestCopy(t *testing.T) {
	t.Parallel()

	scanError := errors.New("scanError")
	batchWriteError := errors.New("batchWriteError")

	testCases := []struct {
		subTestName   string
		mocker        func(src, trg *mocks.DynamoDBService)
		totalReaders  int
		totalWriters  int
		expectedError error
	}{
		{
			"ScanError",
			func(src, trg *mocks.DynamoDBService) {
				src.On("Scan", mock.AnythingOfType("chan []dynamodbcopy.DynamoDBItem"), 1, 0).Return(scanError).Once()
				trg.On("BatchWrite", mock.AnythingOfType("[]dynamodbcopy.DynamoDBItem")).Return(nil).Maybe()
			},
			1,
			1,
			scanError,
		},
		{
			"BatchWriteError",
			func(src, trg *mocks.DynamoDBService) {
				src.On("Scan", mock.AnythingOfType("chan []dynamodbcopy.DynamoDBItem"), 1, 0).Return(nil).Once()
				trg.On("BatchWrite", mock.AnythingOfType("[]dynamodbcopy.DynamoDBItem")).Return(batchWriteError).Once()
			},
			1,
			1,
			batchWriteError,
		},
		{
			"Success",
			func(src, trg *mocks.DynamoDBService) {
				src.On("Scan", mock.AnythingOfType("chan []dynamodbcopy.DynamoDBItem"), 1, 0).Return(nil).Once()
				trg.On("BatchWrite", mock.AnythingOfType("[]dynamodbcopy.DynamoDBItem")).Return(nil).Once()
			},
			1,
			1,
			nil,
		},
		{
			"MultipleWorkers",
			func(src, trg *mocks.DynamoDBService) {
				src.On("Scan", mock.AnythingOfType("chan []dynamodbcopy.DynamoDBItem"), 3, 0).Return(nil).Once()
				src.On("Scan", mock.AnythingOfType("chan []dynamodbcopy.DynamoDBItem"), 3, 1).Return(nil).Once()
				src.On("Scan", mock.AnythingOfType("chan []dynamodbcopy.DynamoDBItem"), 3, 2).Return(nil).Once()
				trg.On("BatchWrite", mock.AnythingOfType("[]dynamodbcopy.DynamoDBItem")).Return(nil).Times(3)
			},
			3,
			3,
			nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(
			testCase.subTestName,
			func(st *testing.T) {
				src := &mocks.DynamoDBService{}
				trg := &mocks.DynamoDBService{}

				testCase.mocker(src, trg)

				service := dynamodbcopy.NewCopier(src, trg, testCase.totalReaders, testCase.totalWriters)

				err := service.Copy()

				assert.Equal(t, testCase.expectedError, err)

				src.AssertExpectations(st)
				trg.AssertExpectations(st)
			},
		)
	}
}
