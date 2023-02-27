package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"time"
)

func getNextPage(ddb *dynamodb.Client, tableName *string, maybeStartKey map[string]types.AttributeValue) (*dynamodb.ScanOutput, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelFunc()

	req := &dynamodb.ScanInput{
		TableName:         tableName,
		ExclusiveStartKey: maybeStartKey,
	}
	return ddb.Scan(ctx, req)
}

func AsyncScanTable(ddb *dynamodb.Client, tableName *string) (chan map[string]types.AttributeValue, chan error) {
	outCh := make(chan map[string]types.AttributeValue, 100)
	errCh := make(chan error, 1)

	go func() {
		var maybeStartKey map[string]types.AttributeValue
		for {
			response, err := getNextPage(ddb, tableName, maybeStartKey)
			if err != nil {
				log.Print("ERROR Could not get next page from Dynamo: ", err)
				errCh <- err
				close(outCh)
				return
			}

			for _, item := range response.Items {
				outCh <- item
			}
			if response.LastEvaluatedKey == nil {
				log.Print("INFO Came to the end of the Dynamo table")
				close(outCh)
				return
			}
			maybeStartKey = response.LastEvaluatedKey
		}
	}()
	return outCh, errCh
}
