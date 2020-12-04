package elastic

import (
	"datawaves/secrets"
	"datawaves/util"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v7"
	"github.com/google/uuid"
	nrelasticsearch "github.com/newrelic/go-agent/v3/integrations/nrelasticsearch-v7"
)

var client *elasticsearch.Client

func init() {
	var err error

	if util.IsProduction() {
		// "github.com/newrelic/go-agent/v3/integrations/nrelasticsearch-v7"
		url, err := secrets.Get("elasticsearch_url")
		if err != nil {
			panic(err)
		}

		user, err := secrets.Get("elasticsearch_user")
		if err != nil {
			panic(err)
		}

		pass, err := secrets.Get("elasticsearch_pass")
		if err != nil {
			panic(err)
		}

		cfg := elasticsearch.Config{
			Addresses: []string{
				url,
			},
			Transport: nrelasticsearch.NewRoundTripper(nil),
			Username:  user,
			Password:  pass,
		}

		client, err = elasticsearch.NewClient(cfg)
	} else {
		cfg := elasticsearch.Config{
			Addresses: []string{
				os.Getenv("elasticsearch_testing_url"),
			},
			Username: os.Getenv("elasticsearch_testing_user"),
			Password: os.Getenv("elasticsearch_testing_pass"),
		}

		client, err = elasticsearch.NewClient(cfg)
	}

	if err != nil {
		log.Fatalf("Error conntecting to ElasticSearch: %s", err)
		panic(err)
	}
}

func GetIndex(projectID, collection string) string {
	p := strings.ReplaceAll(projectID, "-", "")
	return strings.ToLower(p + collection)
}

func GetUsageIndex(projectID string) string {
	p := strings.ReplaceAll(projectID, "-", "")
	return strings.ToLower(p + "datawavesapiusage")
}

func GetID() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

// returns the interval type if valid
func IsValidInterval(interval string) (bool, string) {
	if interval == "" {
		return false, ""
	}

	iv := strings.ToLower(interval)
	if iv == "minute" ||
		iv == "hour" ||
		iv == "day" ||
		iv == "week" ||
		iv == "month" ||
		iv == "quarter" ||
		iv == "year" {
		return true, "calendar"
	}

	s := strings.Split(iv, "ms")
	if len(s) == 2 {
		_, err := strconv.Atoi(s[0])
		if err == nil {
			return true, "fixed"
		} else {
			return false, ""
		}
	}

	s = strings.Split(iv, "s")
	if len(s) == 2 {
		_, err := strconv.Atoi(s[0])
		if err == nil {
			return true, "fixed"
		} else {
			return false, ""
		}
	}

	s = strings.Split(iv, "m")
	if len(s) == 2 {
		_, err := strconv.Atoi(s[0])
		if err == nil {
			return true, "fixed"
		} else {
			return false, ""
		}
	}

	s = strings.Split(iv, "h")
	if len(s) == 2 {
		_, err := strconv.Atoi(s[0])
		if err == nil {
			return true, "fixed"
		} else {
			return false, ""
		}
	}

	s = strings.Split(iv, "d")
	if len(s) == 2 {
		_, err := strconv.Atoi(s[0])
		if err == nil {
			return true, "fixed"
		} else {
			return false, ""
		}
	}

	return false, ""
}

type Filter struct {
	PropertyName  string      `json:"property_name"`
	Operator      string      `json:"operator"`
	PropertyValue interface{} `json:"property_value"`
	Operands      []Filter    `json:"operands"`
}

type Order struct {
	By        string `json:"by"`
	Direction string `json:"direction"`
}

type Timeframe struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type Search struct {
	Index            string
	Timeframe        Timeframe `json:"timeframe"`
	Interval         string    `json:"interval"`
	Timezone         string    `json:"timezone"`
	Filters          []Filter  `json:"filters"`
	MustFilters      []string
	MustNotFilters   []string
	ShouldFilters    []string
	ShouldNotFilters []string
	TargetProperty   string `json:"target_property"`
	GroupBy          string `json:"group_by"`
	Mapping          map[string]string
	Order            Order `json:"order"`
}

func appendFilter(filters []string, filter Filter) []string {
	if filter.Operator == "eq" {
		return append(filters, fmt.Sprintf("{\"term\":{\"%s.keyword\":\"%s\"}}", filter.PropertyName, filter.PropertyValue))
	}

	if filter.Operator == "ne" {
		return append(filters, fmt.Sprintf("{\"term\":{\"%s\":\"%s\"}}", filter.PropertyName, filter.PropertyValue))
	}

	if filter.Operator == "lt" {
		// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
		return append(filters, fmt.Sprintf("{\"range\":{\"%s\":{\"lt\":%v}}}", filter.PropertyName, filter.PropertyValue))
	}

	if filter.Operator == "lte" {
		// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
		return append(filters, fmt.Sprintf("{\"range\":{\"%s\":{\"lte\":%v}}}", filter.PropertyName, filter.PropertyValue))
	}

	if filter.Operator == "gt" {
		// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
		return append(filters, fmt.Sprintf("{\"range\":{\"%s\":{\"gt\":%v}}}", filter.PropertyName, filter.PropertyValue))
	}

	if filter.Operator == "gte" {
		// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
		return append(filters, fmt.Sprintf("{\"range\":{\"%s\":{\"gte\":%v}}}", filter.PropertyName, filter.PropertyValue))
	}

	if filter.Operator == "exists" {
		if filter.PropertyValue == "false" {
			return append(filters, fmt.Sprintf("{\"exists\":{\"field\":\"%s\"}}", filter.PropertyName))
		} else {
			return append(filters, fmt.Sprintf("{\"exists\":{\"field\":\"%s\"}}", filter.PropertyName))
		}
	}

	if filter.Operator == "contains" {
		// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html
		return append(filters, fmt.Sprintf("{\"wildcard\":{\"%s\":{\"value\":\"*%s*\"}}}", filter.PropertyName, filter.PropertyValue))
	}

	if filter.Operator == "not_contains" {
		// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html
		return append(filters, fmt.Sprintf("{\"wildcard\":{\"%s\":{\"value\":\"*%s*\"}}}", filter.PropertyName, filter.PropertyValue))
	}

	// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html
	if filter.Operator == "regexp" || filter.Operator == "regex" {
		return append(filters, fmt.Sprintf("{\"regexp\": {\"%s\": {\"value\":\"%s\"}}}", filter.PropertyName, filter.PropertyValue))
	}

	if filter.Operator == "in" {
		var value string
		switch arr := filter.PropertyValue.(type) {
		case []interface{}:
			value = "["
			for i := range arr {
				switch item := arr[i].(type) {
				case int:
					if i == len(arr)-1 {
						value = value + fmt.Sprintf("%d", item)
					} else {
						value = value + fmt.Sprintf("%d,", item)
					}
				case float32:
					if i == len(arr)-1 {
						value = value + fmt.Sprintf("%f", item)
					} else {
						value = value + fmt.Sprintf("%f,", item)
					}
				case float64:
					if i == len(arr)-1 {
						value = value + fmt.Sprintf("%f", item)
					} else {
						value = value + fmt.Sprintf("%f,", item)
					}
				case string:
					if i == len(arr)-1 {
						value = value + fmt.Sprintf("\"%s\"", item)
					} else {
						value = value + fmt.Sprintf("\"%s\",", item)
					}
				}
			}
			value = value + "]"
		}

		if value != "" {
			// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html
			return append(filters, fmt.Sprintf("{\"terms\":{\"%s\": %s}}", filter.PropertyName, value))
		}
	}

	return filters
}

func GetFilterQuery(name string, filters []string) string {
	var query string
	happened := make(map[string]bool)
	if len(filters) > 0 {
		query = fmt.Sprintf("\"%s\":", name)
		if len(filters) > 1 {
			query = query + "["
			for i, filter := range filters {
				if happened[filter] == true {
					continue
				}
				query = query + filter
				if i != len(filters)-1 {
					query = query + ","
				}
				happened[filter] = true
			}
			query = query + "]"
		} else {
			// only one filter
			query = query + filters[0]
		}
	}

	return query
}

func GetShouldFilterQuery(search *Search) string {

	must := GetFilterQuery("must", search.ShouldFilters)
	mustnot := GetFilterQuery("must_not", search.ShouldNotFilters)

	if must != "" && mustnot != "" {
		return "\"should\":{\"bool\":{" + must + "," + mustnot + "}}"
	}

	if must != "" {
		return "\"should\":{\"bool\":{" + must + "}}"
	}

	if mustnot != "" {
		return "\"should\":{\"bool\":{" + mustnot + "}}"
	}

	return ""
}

func (search *Search) GetQuery(op string) string {
	var err error
	if search.GroupBy != "" || op == "cardinality" {
		search.Mapping, err = GetMapping(search.Index)
		if err != nil {
			search.GroupBy = ""
		}
	}

	intervalType := ""
	isValid := false
	if search.Interval != "" {
		isValid, intervalType = IsValidInterval(search.Interval)
		if !isValid {
			search.Interval = ""
		}
	}

	if len(search.Filters) > 0 {
		for _, filter := range search.Filters {

			if filter.Operator == "or" && len(filter.Operands) > 0 {
				for _, operand := range filter.Operands {
					if operand.Operator == "eq" ||
						operand.Operator == "lt" ||
						operand.Operator == "lte" ||
						operand.Operator == "gt" ||
						operand.Operator == "gte" ||
						operand.Operator == "contains" ||
						operand.Operator == "in" ||
						operand.Operator == "regexp" || operand.Operator == "regex" ||
						(operand.Operator == "exists" && operand.PropertyValue == "true") {
						search.ShouldFilters = appendFilter(search.ShouldFilters, operand)
					}

					if operand.Operator == "ne" ||
						operand.Operator == "not_contains" ||
						(operand.Operator == "exists" && operand.PropertyValue == "false") {
						search.ShouldNotFilters = appendFilter(search.ShouldNotFilters, operand)
					}
				}
			}

			if filter.Operator == "eq" ||
				filter.Operator == "lt" ||
				filter.Operator == "lte" ||
				filter.Operator == "gt" ||
				filter.Operator == "gte" ||
				filter.Operator == "contains" ||
				filter.Operator == "in" ||
				filter.Operator == "regexp" || filter.Operator == "regex" ||
				(filter.Operator == "exists" && filter.PropertyValue == "true") {
				search.MustFilters = appendFilter(search.MustFilters, filter)
			}

			if filter.Operator == "ne" ||
				filter.Operator == "not_contains" ||
				(filter.Operator == "exists" && filter.PropertyValue == "false") {
				search.MustNotFilters = appendFilter(search.MustNotFilters, filter)
			}
		}
	}

	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-min-aggregation.html
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-max-aggregation.html
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-sum-aggregation.html
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-avg-aggregation.html
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-cardinality-aggregation.html
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-extendedstats-aggregation.html
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-median-absolute-deviation-aggregation.html
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-percentile-aggregation.html

	aggs := ""
	if ((op == "count" && search.GroupBy != "") || (op == "count" && search.Interval != "")) || op == "min" || op == "max" || op == "sum" || op == "avg" || op == "cardinality" || op == "percentiles" || op == "extended_stats" || op == "median_absolute_deviation" {
		order := ""

		// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-datehistogram-aggregation.html
		interval := ""
		if search.Interval != "" {
			timezone := ""
			if search.Timezone != "" {
				timezone = fmt.Sprintf(",\"time_zone\":\"%s\"", search.Timezone)
			}
			interval = fmt.Sprintf("\"date_histogram\":{\"field\":\"datawaves.timestamp\", \"%s_interval\":\"%s\"%s}", intervalType, search.Interval, timezone)
		}

		if op == "count" && search.Interval != "" {
			aggs = fmt.Sprintf(",\"aggs\":{\"result\":{%s}}", interval)
			goto Jump
		}

		if op == "cardinality" && search.Mapping != nil && search.Mapping[search.TargetProperty] == "text" {
			aggs = fmt.Sprintf(",\"aggs\":{\"%s_value\":{\"%s\":{\"field\":\"%s.keyword\"}}}", op, op, search.TargetProperty)
		} else {
			aggs = fmt.Sprintf(",\"aggs\":{\"%s_value\":{\"%s\":{\"field\":\"%s\"}}}", op, op, search.TargetProperty)
		}

		// if interval exist, ignore order and group by
		if interval != "" {
			aggs = fmt.Sprintf(",\"aggs\":{\"result\":{%s %s}}", interval, aggs)
			goto Jump
		}

		if (strings.ToLower(search.Order.By) == "key" || strings.ToLower(search.Order.By) == "count") && (strings.ToLower(search.Order.Direction) == "desc" || strings.ToLower(search.Order.Direction) == "asc") {
			order = fmt.Sprintf(",\"order\":{\"_%s\":\"%s\"}", search.Order.By, search.Order.Direction)
		}

		if search.GroupBy != "" && search.Mapping[search.GroupBy] != "" {
			if op == "count" {
				aggs = ""
			}
			if search.Mapping[search.GroupBy] == "text" {
				aggs = fmt.Sprintf(",\"aggs\":{\"result\":{\"terms\":{\"field\":\"%s.keyword\" %s} %s}}", search.GroupBy, order, aggs)
			} else {
				aggs = fmt.Sprintf(",\"aggs\":{\"result\":{\"terms\":{\"field\":\"%s\" %s} %s}}", search.GroupBy, order, aggs)
			}
		}
	}

Jump:
	// Dates should be in this format 'YYYY-MM-DDTHH:mm:ss.sssZ' like '2020-01-30T00:00:00.000Z'
	timeframe := ""
	if search.Timeframe.From != "" && search.Timeframe.To != "" {
		_, err := time.Parse(time.RFC3339, search.Timeframe.From)
		if err != nil {
			search.Timeframe.From = ""
			search.Timeframe.To = ""
		}

		_, err = time.Parse(time.RFC3339, search.Timeframe.To)
		if err != nil {
			search.Timeframe.From = ""
			search.Timeframe.To = ""
		}

		timezone := ""
		if search.Timezone != "" {
			timezone = fmt.Sprintf(",\"time_zone\":\"%s\"", search.Timezone)
		}

		// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
		if search.Timeframe.From != "" || search.Timeframe.To != "" {
			timeframe = fmt.Sprintf("{\"range\":{\"datawaves.timestamp\": {\"gte\":\"%s\",\"lte\":\"%s\" %s}}}", search.Timeframe.From, search.Timeframe.To, timezone)
		}
	} else {
		timezone := ""
		if search.Timezone != "" {
			timezone = fmt.Sprintf(",\"time_zone\":\"%s\"", search.Timezone)
		}
		timeframe = fmt.Sprintf("{\"range\":{\"datawaves.timestamp\": {\"lte\":\"now\" %s}}}", timezone)
	}

	search.MustFilters = append(search.MustFilters, timeframe)
	must := GetFilterQuery("must", search.MustFilters)
	mustnot := GetFilterQuery("must_not", search.MustNotFilters)
	should := GetShouldFilterQuery(search)

	// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html
	if must != "" && mustnot != "" && should != "" {
		return fmt.Sprintf("{\"query\":{\"bool\":{%s,%s,%s}} %s}", must, mustnot, should, aggs)
	}

	if must != "" && mustnot != "" {
		return fmt.Sprintf("{\"query\":{\"bool\":{%s,%s}} %s}", must, mustnot, aggs)
	}

	if must != "" && should != "" {
		return fmt.Sprintf("{\"query\":{\"bool\":{%s, %s}} %s}", must, should, aggs)
	}

	if mustnot != "" && should != "" {
		return fmt.Sprintf("{\"query\":{\"bool\":{%s, %s}} %s}", mustnot, should, aggs)
	}

	if must != "" {
		return fmt.Sprintf("{\"query\":{\"bool\":{%s}} %s}", must, aggs)
	}

	if mustnot != "" {
		return fmt.Sprintf("{\"query\":{\"bool\":{%s}} %s}", mustnot, aggs)
	}

	if should != "" {
		return fmt.Sprintf("{\"query\":{\"bool\":{%s}} %s}", should, aggs)
	}

	if aggs != "" {
		return fmt.Sprintf("{\"query\": %s}", aggs)
	}

	return "{}"
}
