package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"log"
)

func main() {
	tableName := flag.String("table", "", "Name of the table to capture")
	outputNameOverride := flag.String("output", "", "[optional] File to output. If not specified, will default to {tableName}.json")
	flag.Parse()

	if *tableName == "" {
		log.Fatal("ERROR You must specify a table to read")
	}
	awsConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal("ERROR Could not get AWS config: ", err)
	}

	ddb := dynamodb.NewFromConfig(awsConfig)

	recordsCh, errCh := AsyncScanTable(ddb, tableName)
	outputName := fmt.Sprintf("%s.json", *tableName)
	if *outputNameOverride != "" {
		outputName = *outputNameOverride
	}

	writeErrCh := AsyncNdjsonWriter(recordsCh, outputName)
	for {
		select {
		case dbErr := <-errCh:
			log.Printf("ERROR from database read: %s", dbErr)

		case writeErr, stillWriting := <-writeErrCh:
			if writeErr != nil {
				log.Printf("ERROR from writer: %s", writeErr)
			}
			if !stillWriting {
				log.Printf("INFO All done")
				return
			}
		}
	}
}
