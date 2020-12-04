package elastic

import (
	"datawaves/errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/elastic/go-elasticsearch/v7/esapi"
	jsoniter "github.com/json-iterator/go"
)

var opNamesMap = map[string]string{
	"sum":            "sum",
	"min":            "min",
	"max":            "max",
	"avg":            "avg",
	"cardinality":    "count",
	"percentiles":    "percentiles",
	"extended_stats": "standard_deviation",
	"median":         "median",
}

func aggs(r *http.Request, op, idx, body string) (interface{}, error) {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	var search Search
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

	_aggs, ok := rr["aggregations"].(map[string]interface{})
	if !ok {
		errors.Log(errors.New(fmt.Sprintf("Assertion error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res)))
		return nil, errors.New("Assertion error!")
	}

	L := ""
	if op == "extended_stats" {
		L = "std_deviation"
	} else {
		L = "value"
	}

	if search.Interval != "" {
		rslt, ok := _aggs["result"].(map[string]interface{})
		if !ok {
			goto Value
		}

		rs, ok := rslt["buckets"].([]interface{})
		if !ok {
			goto Value
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

				if k == "key_as_string" {
					x["start"] = v
				}

				if k == op+"_value" {
					z, ok := v.(map[string]interface{})
					if !ok {
						continue
					}

					x[opNamesMap[op]] = z[L]

				}
			}
			y = append(y, x)
		}

		return y, nil
	}

	if search.GroupBy != "" {
		rslt, ok := _aggs["result"].(map[string]interface{})
		if !ok {
			goto Value
		}

		rs, ok := rslt["buckets"].([]interface{})
		if !ok {
			goto Value
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

				if k == "key" {

					x[search.GroupBy] = v
				}

				if k == op+"_value" {
					z, ok := v.(map[string]interface{})
					if !ok {
						continue
					}

					x[opNamesMap[op]] = z[L]

				}
			}
			y = append(y, x)
		}

		return y, nil
	}

Value:
	aggs, ok := _aggs[op+"_value"].(map[string]interface{})
	if !ok {
		errors.Log(errors.New(fmt.Sprintf("Assertion error. Index: %s.\n Body: %s.\n Query: %s.\n Response: %v.\n", idx, body, query, res)))
		return nil, errors.New("Assertion error!")
	}

	switch val := aggs[L].(type) {
	case int:
		return float64(val), nil
	case float32:
		return float64(val), nil
	case float64:
		return val, nil
	default:
		return 0.0, nil
	}
}

func Min(r *http.Request, idx, body string) (interface{}, error) {
	return aggs(r, "min", idx, body)
}

func Max(r *http.Request, idx, body string) (interface{}, error) {
	return aggs(r, "max", idx, body)
}

func Avg(r *http.Request, idx, body string) (interface{}, error) {
	return aggs(r, "avg", idx, body)
}

func Sum(r *http.Request, idx, body string) (interface{}, error) {
	return aggs(r, "sum", idx, body)
}

func CountUnique(r *http.Request, idx, body string) (interface{}, error) {
	return aggs(r, "cardinality", idx, body)
}

func Median(r *http.Request, idx, body string) (interface{}, error) {
	return aggs(r, "median_absolute_deviation", idx, body)
}

func StandardDeviation(r *http.Request, idx, body string) (interface{}, error) {
	return aggs(r, "extended_stats", idx, body)
}
