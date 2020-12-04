package elastic

import (
	"context"
	"datawaves/errors"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/elastic/go-elasticsearch/v7/esapi"
)

func GetCollections(projectID string) []string {
	collections := []string{}

	// Set up the request object.
	req := esapi.CatIndicesRequest{
		Index: []string{projectID + "*"},
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), client)
	if err != nil {
		errors.Log(err, fmt.Sprintf("Error getting response.\nProjectID: %s\nResponse: %v.\n", projectID, res))
		return collections
	}
	defer res.Body.Close()

	if res.IsError() {
		errors.Log(errors.New(fmt.Sprintf("Response error.\nProjectID: %s\nResponse: %v.\n", projectID, res)))
		return collections
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		errors.Log(err, fmt.Sprintf("Decoding error.\nProjectID: %s\nResponse: %v.\n", projectID, res))
		return collections
	}

	// body is returned in the following format:
	// yellow open bp745pq23akg01vmgfm0posts nFvV9EsGR3aIzJS6INISDw 1 1  2 0 11.1kb 11.1kb

	// https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html

	re := regexp.MustCompile(projectID + `[\w\d]*[-_]*[\w\d]*`)

	all := re.FindAll(body, -1)
	for i := range all {
		idx := string(all[i])
		collection := strings.Split(idx, projectID)[1]
		if collection == projectID {
			continue
		}
		collections = append(collections, strings.TrimSpace(collection))
	}

	return collections
}
