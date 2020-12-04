package elastic

import (
	"datawaves/errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/elastic/go-elasticsearch/v7/esapi"
	jsoniter "github.com/json-iterator/go"
)

// timeframe
// filters
// timezone
// group_by
// interval
func Count(r *http.Request, idx, body string) (interface{}, error) {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	var search Search
	err := json.NewDecoder(strings.NewReader(body)).Decode(&search)
	if err != nil {
		errors.Log(err)
		return 0, errors.New("Error decoding request body!")
	}
	search.Index = idx

	query := search.GetQuery("count")

	if search.GroupBy != "" || search.Interval != "" {
		return _count(r, idx, body, query, &search)
	}

	// Set up the request object.
	req := esapi.CountRequest{
		Index: []string{idx},
		Body:  strings.NewReader(query),
	}

	// Perform the request with the client.
	res, err := req.Do(r.Context(), client)
	if err != nil {
		errors.Log(err, fmt.Sprintf("Error getting response. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res))
		return 0, errors.New("Error counting documents!")
	}
	defer res.Body.Close()

	if res.IsError() {
		errors.Log(errors.New(fmt.Sprintf("Response error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res)))
		return 0, errors.New("Failed to count documents!")
	}

	var rr map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&rr)
	if err != nil {
		errors.Log(err, fmt.Sprintf("Decoding error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res))
		return 0, errors.New("Error decoding response!")
	}

	count, ok := rr["count"].(float64)
	if !ok {
		errors.Log(errors.New(fmt.Sprintf("Assertion error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res)))
		return 0, errors.New("Assertion error!")
	}

	return count, nil
}

func _count(r *http.Request, idx, body, query string, search *Search) (interface{}, error) {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary

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

	_aggs, ok := rr["aggregations"].(map[string]interface{})
	if !ok {
		errors.Log(errors.New(fmt.Sprintf("Assertion error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res)))
		return nil, errors.New("Assertion error!")
	}

	rslt, ok := _aggs["result"].(map[string]interface{})
	if !ok {
		errors.Log(errors.New(fmt.Sprintf("Assertion error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res)))
		return nil, errors.New("Assertion error!")
	}

	rs, ok := rslt["buckets"].([]interface{})
	if !ok {
		errors.Log(errors.New(fmt.Sprintf("Assertion error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res)))
		return nil, errors.New("Assertion error!")
	}

	y := []map[string]interface{}{}
	for i := range rs {
		x := make(map[string]interface{})

		yy, ok := rs[i].(map[string]interface{})
		if !ok {
			continue
		}
		for k, v := range yy {
			if k == "doc_count" {
				x["count"] = v
			}

			if search.Interval != "" {
				if k == "key_as_string" {
					x["start"] = v
				}
			} else {
				if k == "key" {
					x[search.GroupBy] = v
				}
			}

		}
		y = append(y, x)
	}

	return y, nil
}
