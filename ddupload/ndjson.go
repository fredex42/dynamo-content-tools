package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"os"
	"reflect"
	"strconv"
)

func recursivelyHandleValue(valueMap map[string]interface{}) (types.AttributeValue, error) {
	//output := make(map[string]types.AttributeValue, len(valueMap))
	if value, hasValue := valueMap["Value"]; hasValue {
		if stringValue, isString := value.(string); isString {
			_, err := strconv.ParseInt(stringValue, 10, 64)
			if err == nil {
				output := &types.AttributeValueMemberN{Value: stringValue}
				return output, nil
			}
			_, err = strconv.ParseFloat(stringValue, 64)
			if err == nil {
				output := &types.AttributeValueMemberN{Value: stringValue}
				return output, nil
			}
			return &types.AttributeValueMemberS{Value: stringValue}, nil
		} else if boolValue, isBool := value.(bool); isBool {
			return &types.AttributeValueMemberBOOL{Value: boolValue}, nil
		} else if mapValue, isMap := value.(map[string]interface{}); isMap {
			subOutput := make(map[string]types.AttributeValue, 0)
			for k, subValue := range mapValue {
				if subMap, isSubMap := subValue.(map[string]interface{}); isSubMap {
					result, err := recursivelyHandleValue(subMap)
					subOutput[k] = result
					if err != nil {
						return nil, err
					}
				}
			}
			return &types.AttributeValueMemberM{Value: subOutput}, nil
		} else {
			return nil, errors.New(fmt.Sprintf("did not recognise data type %s", reflect.TypeOf(value)))
		}
	} else {
		return nil, errors.New("value did not have a 'Value' key")
	}
}

func recursivelyParse(intermediate map[string]map[string]interface{}) (map[string]types.AttributeValue, error) {
	output := make(map[string]types.AttributeValue, len(intermediate))
	for k, valueMap := range intermediate {
		result, err := recursivelyHandleValue(valueMap)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("could not handle key %s: %s", k, err))
		}
		output[k] = result
	}
	return output, nil
}

func customisedUnmarshal(input []byte) (map[string]types.AttributeValue, error) {
	var intermediate map[string]map[string]interface{}

	err := json.Unmarshal(input, &intermediate)
	if err != nil {
		return nil, err
	}

	return recursivelyParse(intermediate)
}

func AsyncNdjsonReader(filename *string) (chan map[string]types.AttributeValue, chan error) {
	outputCh := make(chan map[string]types.AttributeValue, 100)
	errCh := make(chan error, 1)

	go func() {
		fp, err := os.Open(*filename)
		if err != nil {
			log.Printf("ERROR Could not open %s to read: %s", *filename, err)
			errCh <- err
			close(errCh)
			return
		}
		defer fp.Close()

		scanner := bufio.NewScanner(fp)

		for scanner.Scan() {
			line := scanner.Bytes()
			content, err := customisedUnmarshal(line)
			if err == nil {
				outputCh <- content
			} else {
				errCh <- err
			}
		}
		log.Printf("AsyncNdjsonReader came to the end of the file")
		close(outputCh)
		close(errCh)
	}()
	return outputCh, errCh
}
