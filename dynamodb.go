package dynamodbcopy

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

const maxBatchWriteSize = 25

// DynamoDBAPI just a wrapper over aws-sdk dynamodbiface.DynamoDBAPI interface for mocking purposes
type DynamoDBAPI interface {
	dynamodbiface.DynamoDBAPI
}

type DynamoDBService interface {
	DescribeTable() (*dynamodb.TableDescription, error)
	UpdateCapacity(capacity Capacity) error
	WaitForReadyTable() error
	BatchWrite(requests []*dynamodb.WriteRequest) error
	Scan(items ItemsChan, totalSegments, segment int64) error
}

type dynamoDBSerivce struct {
	tableName string
	api       DynamoDBAPI
	sleep     Sleeper
}

func NewDynamoDBAPI(profile string) DynamoDBAPI {
	options := session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}

	if profile != "" {
		options.Profile = profile
	}

	return dynamodb.New(
		session.Must(
			session.NewSessionWithOptions(
				options,
			),
		),
	)
}

func NewDynamoDBService(tableName string, api DynamoDBAPI, sleepFn Sleeper) DynamoDBService {
	return dynamoDBSerivce{tableName, api, sleepFn}
}

func (db dynamoDBSerivce) DescribeTable() (*dynamodb.TableDescription, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(db.tableName),
	}

	output, err := db.api.DescribeTable(input)
	if err != nil {
		return nil, err
	}

	return output.Table, nil
}

func (db dynamoDBSerivce) UpdateCapacity(capacity Capacity) error {
	read := capacity.Read
	write := capacity.Write

	if read == 0 || write == 0 {
		return fmt.Errorf(
			"invalid update capacity read %d, write %d: capacity units must be greater than 0",
			read,
			write,
		)
	}

	input := &dynamodb.UpdateTableInput{
		TableName: aws.String(db.tableName),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(read),
			WriteCapacityUnits: aws.Int64(write),
		},
	}

	_, err := db.api.UpdateTable(input)
	if err != nil {
		return err
	}

	return db.WaitForReadyTable()
}

func (db dynamoDBSerivce) BatchWrite(requests []*dynamodb.WriteRequest) error {
	if len(requests) == 0 {
		return nil
	}

	var remainingRequests []*dynamodb.WriteRequest
	for _, request := range requests {
		if len(remainingRequests) == maxBatchWriteSize {
			if err := db.batchWriteItem(remainingRequests); err != nil {
				return err
			}

			remainingRequests = nil
		}

		remainingRequests = append(remainingRequests, request)
	}

	if len(remainingRequests) == 0 {
		return nil
	}

	return db.batchWriteItem(remainingRequests)
}

func (db dynamoDBSerivce) batchWriteItem(requests []*dynamodb.WriteRequest) error {
	tableName := db.tableName

	writeRequests := requests
	for {
		batchInput := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				tableName: writeRequests,
			},
		}

		output, err := db.api.BatchWriteItem(batchInput)
		if err != nil {
			return err
		}

		writeRequests = output.UnprocessedItems[tableName]
		if len(writeRequests) == 0 {
			break
		}
	}

	return nil
}

func (db dynamoDBSerivce) WaitForReadyTable() error {
	elapsed := 0

	for attempt := 0; ; attempt++ {
		description, err := db.DescribeTable()
		if err != nil {
			return err
		}

		if *description.TableStatus == dynamodb.TableStatusActive {
			break
		}

		elapsed += db.sleep(elapsed * attempt)
	}

	return nil
}

type ItemsChan chan map[string]*dynamodb.AttributeValue

func (db dynamoDBSerivce) Scan(items ItemsChan, totalSegments, segment int64) error {
	if totalSegments == 0 {
		return errors.New("totalSegments has to be greater than 0")
	}

	input := dynamodb.ScanInput{
		TableName: aws.String(db.tableName),
	}

	if totalSegments > 1 {
		input.SetSegment(segment)
		input.SetTotalSegments(totalSegments)
	}

	return db.api.ScanPages(
		&input,
		func(output *dynamodb.ScanOutput, b bool) bool {
			for _, item := range output.Items {
				items <- item
			}

			return !b
		},
	)
}
