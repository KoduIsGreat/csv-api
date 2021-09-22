package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
)
var csvFile  = flag.String("f", "", "path to csv file")
func main() {
	flag.Parse()
	fs, err := os.Open(*csvFile)
	if err != nil {
		log.Fatal("error opening file")
	}
	data, err := csvFileToMap(fs)
	if err != nil {
		log.Fatal("error reading csv file")
	}
	if err := server(data); err != nil {
		log.Fatalf("failed to start server %v", err)
	}
}


func server(data []map[string]string) error {
	// get all the headers available
	allHeaders := getHeaders([]string{}, data...)
	count := len(data)
	http.HandleFunc("/detail", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		type response struct {
			Data []string `json:"data"`
			Count int `json:"numRecords"`
		}
		res := response{Data: allHeaders, Count: count}
		w.Header().Set("Content-Type", "application/json")
		if err :=json.NewEncoder(w).Encode(&res); err != nil {
			log.Printf("could not encode %v", res.Data)
		}
	})
	http.HandleFunc("/records", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		type recordsRequest struct {
			Offset *int `json:"offset,omitempty"`
			Limit  *int `json:"limit"`
		}
		type response struct {
			Data []map[string]string `json:"data"`
		}
		var body recordsRequest
		var offset, limit int
		var res response
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "",http.StatusUnprocessableEntity)
			return
		}
		if body.Offset != nil {
			offset = *body.Offset
		}
		if body.Limit != nil {
			limit = *body.Limit
		}
		// gaurd against invalid slice indexing
		if  limit <= 0 || limit > count{
			limit = count
		}
		res.Data = data[offset:limit]
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(&res); err != nil {
			http.Error(w, "",http.StatusUnprocessableEntity)
			return
		}

	})
	return http.ListenAndServe(":8080", nil)
}

func getHeaders(filterFields []string, inputSliceMap ...map[string]string) []string {
	var headers []string
	// iter over slice to get all possible keys (csv header) in the maps
	// using empty Map[string]struct{} to get UNIQUE Keys; no value needed
	var headerMap = make(map[string]struct{})
	for _, record := range inputSliceMap {
		for k, _ := range record {
			headerMap[k] = struct{}{}
		}
	}

	// convert unique headersMap to slice
	for headerValue, _ := range headerMap {
		headers = append(headers, headerValue)
	}

	// filter to filteredFields and maintain order
	var filteredHeaders []string
	if len(filterFields) > 0 {
		for _, filterField := range filterFields {
			for _, headerValue := range headers {
				if filterField == headerValue {
					filteredHeaders = append(filteredHeaders, headerValue)
				}
			}
		}
	} else {
		filteredHeaders = append(filteredHeaders, headers...)
		sort.Strings(filteredHeaders) // alpha sort headers
	}
	return filteredHeaders
}

// reads csv file into slice of map
// slice index is the line number
// map[string]string where key is column name
func csvFileToMap(fs io.Reader) (returnMap []map[string]string, err error) {
	// read csv file

	reader := csv.NewReader(fs)

	rawCSVdata, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf(err.Error())
	}

	header := []string{} // holds first row (header)  better to declare this way to avoid nil indexing below
	for lineNum, record := range rawCSVdata {

		// for first row, build the header slice
		if lineNum == 0 {
			for i := 0; i < len(record); i++ {
				header = append(header, strings.ToLower(strings.TrimSpace(record[i])))
			}
		} else {
			// for each cell, map[string]string k=header v=value
			line := map[string]string{}
			for i := 0; i < len(record); i++ {
				line[header[i]] = record[i]
			}
			returnMap = append(returnMap, line)
		}
	}

	return
}
