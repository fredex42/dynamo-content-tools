package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"time"
)

func commitInBulk(ddb *dynamodb.Client, tableName string, cache *[]map[string]types.AttributeValue) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelFunc()
	requestItems := make([]types.WriteRequest, len(*cache))
	for i, rec := range *cache {
		requestItems[i] = types.WriteRequest{PutRequest: &types.PutRequest{Item: rec}}
	}
	req := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: requestItems,
		},
	}
	_, err := ddb.BatchWriteItem(ctx, req)
	return err
}

func AsyncDynamoWriter(ddb *dynamodb.Client, tableName string, input chan map[string]types.AttributeValue) chan error {
	errCh := make(chan error, 1)

	go func() {
		var cache = make([]map[string]types.AttributeValue, 0)

		for {
			rec, moreRecords := <-input
			if rec != nil {
				cache = append(cache, rec)
				if len(cache) == 25 {
					err := commitInBulk(ddb, tableName, &cache)
					if err != nil {
						errCh <- err
						close(errCh)
						return
					}
					cache = make([]map[string]types.AttributeValue, 0)
				}
			}
			if !moreRecords {
				log.Printf("INFO AsyncDynamoWriter came to the end of the stream")
				err := commitInBulk(ddb, tableName, &cache)

				errCh <- err
				close(errCh)
				return

			}
		}
	}()
	return errCh
}
