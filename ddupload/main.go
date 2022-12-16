package main

import (
	"context"
	"flag"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"log"
)

func main() {
	tableName := flag.String("table", "", "Name of a table to write the data into")
	fileName := flag.String("input", "", "Name of the file to read content from")
	flag.Parse()

	if *tableName == "" {
		log.Fatal("ERROR You must specify a table to write")
	}
	if *fileName == "" {
		log.Fatal("ERROR You must specify a file to read")
	}

	awsConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal("ERROR Could not get AWS config: ", err)
	}

	ddb := dynamodb.NewFromConfig(awsConfig)
	inputCh, readErrCh := AsyncNdjsonReader(fileName)
	writeErrCh := AsyncDynamoWriter(ddb, *tableName, inputCh)

	for {
		select {
		case readErr := <-readErrCh:
			if readErr != nil {
				log.Printf("ERROR from reader: %s", readErr)
			}
		case writeErr, moreWrites := <-writeErrCh:
			if writeErr != nil {
				log.Print("ERROR from writer: ", writeErr)
			}
			if !moreWrites {
				log.Printf("INFO Completed write.")
				return
			}
		}
	}
}
