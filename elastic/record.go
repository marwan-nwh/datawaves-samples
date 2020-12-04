package elastic

import (
	"datawaves/errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7/esapi"
	jsoniter "github.com/json-iterator/go"
)

// Record saves a document in an index
// body: should be a valid json string
func Record(r *http.Request, idx, body string) error {
	id := GetID()

	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	var data map[string]interface{}
	err := json.NewDecoder(strings.NewReader(body)).Decode(&data)
	if err != nil {
		errors.Log(err, "Error decoding data.\nIndex: "+idx+".\nDocument ID: "+id+"."+"\nBody: "+body+".")
		return errors.New("Error decoding document.")
	}

	// year-month-day
	now := time.Now().Format("2006-01-02T15:04:05.000Z")
	timestamp := now

	stamp, ok := data["timestamp"].(string)
	if ok {
		timestamp = stamp
	}

	data["datawaves"] = map[string]interface{}{"id": id, "created_at": now, "timestamp": timestamp}

	input, err := json.Marshal(data)
	if err != nil {
		errors.Log(err, "Error decoding data.\nIndex: "+idx+".\nDocument ID: "+id+"."+"\nBody: "+body+".")
		return errors.New("Error decoding document.")
	}

	// Set up the request object.
	req := esapi.IndexRequest{
		Index:      idx,
		Body:       strings.NewReader(string(input)),
		DocumentID: id,
	}

	// Perform the request with the client.
	res, err := req.Do(r.Context(), client)
	if err != nil {
		errors.Log(err, "Error getting response. Index: "+idx+". Document ID: "+id+"."+" Body: "+body+".")
		return errors.New("Error saving document.")
	}
	defer res.Body.Close()

	if res.IsError() {
		errors.Log(errors.New(fmt.Sprintf("Failed to index document.\nIndex: %s.\nDocument ID: %s.\nBody: %s.\nResponse: %v.", idx, id, body, res)))
		return errors.New("Failed to index document.")
	}

	return nil
}

// RecordBulk saves a bulk of events
// body: should be a valid bulk string
// https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html
func RecordBulk(r *http.Request, body string) error {

	// Set up the request object.
	req := esapi.BulkRequest{
		Body: strings.NewReader(body),
	}

	// Perform the request with the client.
	res, err := req.Do(r.Context(), client)
	if err != nil {
		errors.Log(err, "Error getting response. body: "+body)
		return errors.New("Error saving document.")
	}
	defer res.Body.Close()

	if res.IsError() {
		errors.Log(errors.New(fmt.Sprintf("Failed to index documents. %v", res)))
		return errors.New("Failed to index document.")
	}

	return nil
}
