package elastic

import (
	"datawaves/errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/elastic/go-elasticsearch/v7/esapi"
	jsoniter "github.com/json-iterator/go"
)

func Percentiles(r *http.Request, idx, body string) (map[string]interface{}, error) {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	var search Search
	op := "percentiles"
	err := json.NewDecoder(strings.NewReader(body)).Decode(&search)
	if err != nil {
		errors.Log(err)
		return nil, errors.New("Error decoding request body!")
	}
	search.Index = idx

	if search.TargetProperty == "" {
		return nil, errors.New("Missing target_property!")
	}

	query := search.GetQuery(op)

	size := 0
	// Set up the request object.
	req := esapi.SearchRequest{
		Index: []string{idx},
		Body:  strings.NewReader(query),
		Size:  &size,
	}

	// Perform the request with the client.
	res, err := req.Do(r.Context(), client)
	if err != nil {
		errors.Log(err, fmt.Sprintf("Error getting response. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res))
		return nil, errors.New("Error processing documents!")
	}
	defer res.Body.Close()

	if res.IsError() {
		errors.Log(errors.New(fmt.Sprintf("Response error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res)))
		return nil, errors.New("Failed to process documents!")
	}

	var rr map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&rr)
	if err != nil {
		errors.Log(err, fmt.Sprintf("Decoding error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res))
		return nil, errors.New("Error decoding response!")
	}

	__aggs, ok := rr["aggregations"].(map[string]interface{})
	if !ok {
		errors.Log(errors.New(fmt.Sprintf("Assertion error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res)))
		return nil, errors.New("Assertion error!")
	}

	_aggs, ok := __aggs[op+"_value"].(map[string]interface{})
	if !ok {
		errors.Log(errors.New(fmt.Sprintf("Assertion error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res)))
		return nil, errors.New("Assertion error!")
	}

	aggs, ok := _aggs["values"].(map[string]interface{})
	if !ok {
		errors.Log(errors.New(fmt.Sprintf("Assertion error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res)))
		return nil, errors.New("Assertion error!")
	}

	return aggs, nil
}
