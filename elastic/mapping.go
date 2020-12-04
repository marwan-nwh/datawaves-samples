package elastic

import (
	"context"
	"datawaves/errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/json-iterator/go"
)

func GetMapping(idx string) (map[string]string, error) {
	// Set up the request object.
	req := esapi.IndicesGetMappingRequest{
		Index: []string{idx},
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), client)
	if err != nil {
		errors.Log(err, fmt.Sprintf("Error getting response.\nIndex: %s\nResponse: %v.\n", idx, res))
		return nil, errors.New("Error counting documents!")
	}
	defer res.Body.Close()

	if res.IsError() {
		errors.Log(errors.New(fmt.Sprintf("Response error.\nIndex: %s\nResponse: %v.\n", idx, res)))
		return nil, errors.New("Failed to count documents!")
	}

	var r map[string]interface{}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	err = json.NewDecoder(res.Body).Decode(&r)
	if err != nil {
		errors.Log(err, fmt.Sprintf("Decoding error.\nIndex: %s\nResponse: %v.\n", idx, res))
		return nil, errors.New("Error decoding response!")
	}

	mp, ok := r[idx].(map[string]interface{})
	if !ok {
		errors.Log(errors.New(fmt.Sprintf("Assertion error.\nIndex: %s.\nResponse: %v.\n", idx, res)))
		return nil, errors.New("Assertion error!")
	}

	pr, ok := mp["mappings"].(map[string]interface{})
	if !ok {
		errors.Log(errors.New(fmt.Sprintf("Assertion error.\nIndex: %s.\nResponse: %v.\n", idx, res)))
		return nil, errors.New("Assertion error!")
	}

	fd, ok := pr["properties"].(map[string]interface{})
	if !ok {
		errors.Log(errors.New(fmt.Sprintf("Assertion error.\nIndex: %s.\nResponse: %v.\n", idx, res)))
		return nil, errors.New("Assertion error!")
	}

	fields := make(map[string]string)

	for k, v := range fd {
		if k == "datawaves" {
			continue
		}
		obj, ok := v.(map[string]interface{})
		if !ok {
			errors.Log(errors.New(fmt.Sprintf("Assertion error.\nIndex: %s.\nResponse: %v.\n", idx, res)))
			return nil, errors.New("Assertion error!")
		}

		typ, ok := obj["type"].(string)
		if !ok {
			errors.Log(errors.New(fmt.Sprintf("Assertion error.\nIndex: %s.\nResponse: %v.\n", idx, res)))
			return nil, errors.New("Assertion error!")
		}

		fields[k] = typ
	}

	return fields, nil
}
