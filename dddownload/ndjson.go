package main

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"os"
)

func AsyncNdjsonWriter(input chan map[string]types.AttributeValue, outputFileName string) chan error {
	errCh := make(chan error, 1)

	go func() {
		f, err := os.OpenFile(outputFileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0640)
		if err != nil {
			log.Printf("ERROR Could not open output file '%s': %s", outputFileName, err)
			errCh <- err
			close(errCh)
			return
		}
		defer f.Close()

		for {
			rec, isMore := <-input
			if rec != nil {
				strung, err := json.Marshal(rec)
				if err != nil {
					errCh <- err
				} else {
					f.Write(strung)
					f.Write([]byte("\n"))
				}
			}
			if !isMore {
				log.Printf("INFO All records written\n")
				close(errCh)
				return
			}
		}
	}()
	return errCh
}
