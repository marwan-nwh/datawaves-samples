package elastic

import (
	"datawaves/errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/elastic/go-elasticsearch/v7/esapi"
	jsoniter "github.com/json-iterator/go"
)

// Same as cardinality but fetching the values
func SelectUnique(r *http.Request, idx, body string) ([]interface{}, error) {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	values := []interface{}{}
	valuesMap := make(map[interface{}]bool)
	var search Search
	op := "select_unique"
	err := json.NewDecoder(strings.NewReader(body)).Decode(&search)
	if err != nil {
		errors.Log(err)
		return values, errors.New("Error decoding request body!")
	}
	search.Index = idx

	if search.TargetProperty == "" {
		return values, errors.New("Missing target_property!")
	}

	search.Filters = append(search.Filters, Filter{PropertyName: search.TargetProperty, Operator: "exists", PropertyValue: "true"})

	query := search.GetQuery(op)

	size := 10000
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
		return values, errors.New("Error counting documents!")
	}
	defer res.Body.Close()

	if res.IsError() {
		errors.Log(errors.New(fmt.Sprintf("Response error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res)))
		return values, errors.New("Failed to count documents!")
	}

	var rr map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&rr)
	if err != nil {
		errors.Log(err, fmt.Sprintf("Decoding error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res))
		return values, errors.New("Error decoding response!")
	}

	__hits, ok := rr["hits"].(map[string]interface{})
	if !ok {
		errors.Log(errors.New(fmt.Sprintf("Assertion error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res)))
		return values, errors.New("Assertion error!")
	}

	_hits, ok := __hits["hits"].([]interface{})
	if !ok {
		errors.Log(errors.New(fmt.Sprintf("Assertion error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res)))
		return values, errors.New("Assertion error!")
	}

	if len(_hits) > 0 {
		for _, valObj := range _hits {
			__obj, ok := valObj.(map[string]interface{})
			if !ok {
				errors.Log(errors.New(fmt.Sprintf("Assertion error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res)))
				return values, errors.New("Assertion error!")
			}

			_obj, ok := __obj["_source"].(map[string]interface{})
			if !ok {
				errors.Log(errors.New(fmt.Sprintf("Assertion error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res)))
				return values, errors.New("Assertion error!")
			}

			switch val := _obj[search.TargetProperty].(type) {
			case int:
				if valuesMap[val] == true {
					continue
				}
				values = append(values, val)
				valuesMap[val] = true
			case float32:
				if valuesMap[val] == true {
					continue
				}
				values = append(values, val)
				valuesMap[val] = true
			case float64:
				if valuesMap[val] == true {
					continue
				}
				values = append(values, val)
				valuesMap[val] = true
			case string:
				if valuesMap[val] == true {
					continue
				}
				values = append(values, val)
				valuesMap[val] = true
			}
		}
	}

	return values, nil
}
