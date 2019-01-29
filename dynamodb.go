package dynamodbcopy

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"time"
)

const maxBatchWriteSize = 25

// DynamoDBAPI just a wrapper over aws-sdk dynamodbiface.DynamoDBAPI interface for mocking purposes
type DynamoDBAPI interface {
	dynamodbiface.DynamoDBAPI
}

type DynamoDBItem map[string]*dynamodb.AttributeValue

type DynamoDBService interface {
	DescribeTable() (*dynamodb.TableDescription, error)
	UpdateCapacity(capacity Capacity) error
	WaitForReadyTable() error
	BatchWrite(items []DynamoDBItem) error
	Scan(totalSegments, segment int, itemsChan chan<- []DynamoDBItem) error
}

type dynamoDBSerivce struct {
	tableName string
	api       DynamoDBAPI
	sleep     Sleeper
	logger    Logger
}

func NewDynamoDBAPI(roleArn string) DynamoDBAPI {
	options := session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}

	currentSession := session.Must(session.NewSessionWithOptions(options))
	if roleArn != "" {
		roleCredentials := stscreds.NewCredentials(currentSession, roleArn)

		return dynamodb.New(currentSession, &aws.Config{Credentials: roleCredentials})
	}

	return dynamodb.New(currentSession)
}

func NewDynamoDBService(tableName string, api DynamoDBAPI, sleepFn Sleeper, logger Logger) DynamoDBService {
	return dynamoDBSerivce{tableName, api, sleepFn, logger}
}

func (db dynamoDBSerivce) DescribeTable() (*dynamodb.TableDescription, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(db.tableName),
	}

	output, err := db.api.DescribeTable(input)
	if err != nil {
		return nil, fmt.Errorf("unable to describe table %s: %s", db.tableName, err)
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

	db.logger.Printf("updating %s with read: %d, write: %d", db.tableName, read, write)
	_, err := db.api.UpdateTable(input)
	if err != nil {
		return fmt.Errorf("unable to update table %s: %s", db.tableName, err)
	}

	return db.WaitForReadyTable()
}

func (db dynamoDBSerivce) BatchWrite(items []DynamoDBItem) error {
	db.logger.Printf("writing batch of %d to %s", len(items), db.tableName)
	if len(items) == 0 {
		return nil
	}

	var remainingRequests []*dynamodb.WriteRequest
	for _, item := range items {
		if len(remainingRequests) == maxBatchWriteSize {
			if err := db.batchWriteItem(remainingRequests); err != nil {
				return err
			}

			remainingRequests = nil
		}

		request := &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: item,
			},
		}
		remainingRequests = append(remainingRequests, request)
	}

	return db.batchWriteItem(remainingRequests)
}

func (db dynamoDBSerivce) batchWriteItem(requests []*dynamodb.WriteRequest) error {
	tableName := db.tableName

	writeRequests := requests
	attempt := 0
	elapsedSleepTime := 0
	for len(writeRequests) != 0 {
		batchInput := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				tableName: writeRequests,
			},
		}

		output, err := db.api.BatchWriteItem(batchInput)
		if err != nil {
			if elapsedSleepTime > int(time.Minute)*3 {
				return fmt.Errorf("too many batch wirte retries to table %s: %s", db.tableName, err)
			}

			if awsErr, ok := err.(awserr.Error); ok {
				switch awsErr.Code() {
				case dynamodb.ErrCodeProvisionedThroughputExceededException:
					sleepTime := db.sleep(elapsedSleepTime + attempt)
					db.logger.Printf("batch write provisioning error: waited %d ms (attempt %d) ", sleepTime, attempt)
					attempt++
					elapsedSleepTime += sleepTime

					continue
				case "ThrottlingException":
					sleepTime := db.sleep(int(time.Second) * attempt)
					db.logger.Printf("batch write throttling error: waited %d ms (attempt %d) ", sleepTime, attempt)
					attempt++
					elapsedSleepTime += sleepTime

					continue
				default:
					return fmt.Errorf(
						"aws %s error in batch write to table %s: %s",
						awsErr.Code(),
						db.tableName,
						awsErr.Error(),
					)
				}
			}

			return fmt.Errorf("unable to batch write to table %s: %s", db.tableName, err)
		}

		elapsedSleepTime = 0
		attempt = 0
		writeRequests = output.UnprocessedItems[tableName]
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

func (db dynamoDBSerivce) Scan(totalSegments, segment int, itemsChan chan<- []DynamoDBItem) error {
	if totalSegments == 0 {
		return errors.New("totalSegments has to be greater than 0")
	}

	input := dynamodb.ScanInput{
		TableName: aws.String(db.tableName),
	}

	if totalSegments > 1 {
		input.SetSegment(int64(segment))
		input.SetTotalSegments(int64(totalSegments))
	}

	totalScanned := 0
	pagerFn := func(output *dynamodb.ScanOutput, b bool) bool {
		var items []DynamoDBItem
		for _, item := range output.Items {
			items = append(items, item)
			totalScanned++
		}
		db.logger.Printf("%s table scanned page with %d items (reader %d)", db.tableName, len(items), segment)

		itemsChan <- items

		return !b
	}

	if err := db.api.ScanPages(&input, pagerFn); err != nil {
		return fmt.Errorf("unable to scan table %s: %s", db.tableName, err)
	}

	db.logger.Printf("%s table scanned a total of %d items (reader %d)", db.tableName, totalScanned, segment)

	return nil
}
