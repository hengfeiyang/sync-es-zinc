package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

type ES struct {
	client *elasticsearch.Client
}

func NewES(servers []string, user, pass string) (*ES, error) {
	cfg := elasticsearch.Config{
		Addresses: servers,
		Username:  user,
		Password:  pass,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
			TLSClientConfig: &tls.Config{
				MaxVersion:         tls.VersionTLS11,
				InsecureSkipVerify: true,
			},
		},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &ES{client}, nil
}

func (e *ES) Info() (res *esapi.Response, err error) {
	res, err = e.client.Info()
	if err != nil {
		return nil, fmt.Errorf("error getting response: %s", err)
	}

	// Check response status
	if res.IsError() {
		return nil, fmt.Errorf("error: %s", res.String())
	}
	return res, nil
}

func (e *ES) Search(index string, query []byte) (scrollID string, total int, hits []interface{}, err error) {
	var res *esapi.Response
	buf := bytes.NewBuffer(query)
	res, err = e.client.Search(
		e.client.Search.WithContext(context.Background()),
		e.client.Search.WithIndex(index),
		e.client.Search.WithBody(buf),
		e.client.Search.WithTrackTotalHits(true),
		e.client.Search.WithScroll(time.Second*60),
		e.client.Search.WithPretty(),
	)
	if err != nil {
		err = fmt.Errorf("error getting response: %s", err)
		return
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			err = fmt.Errorf("error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			err = fmt.Errorf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
		return
	}

	var r map[string]interface{}
	if err = json.NewDecoder(res.Body).Decode(&r); err != nil {
		err = fmt.Errorf("error parsing the response body: %s", err)
		return
	}

	scrollID = r["_scroll_id"].(string)
	total = int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))
	hits = r["hits"].(map[string]interface{})["hits"].([]interface{})
	return
}

func (e *ES) Scroll(id string) (scrollID string, hits []interface{}, err error) {
	res, err := e.client.Scroll(
		e.client.Scroll.WithScroll(time.Second*60),
		e.client.Scroll.WithScrollID(id),
		e.client.Scroll.WithPretty(),
	)
	if err != nil {
		return "", nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			err = fmt.Errorf("error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			err = fmt.Errorf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
		return
	}

	var r map[string]interface{}
	if err = json.NewDecoder(res.Body).Decode(&r); err != nil {
		err = fmt.Errorf("error parsing the response body: %s", err)
		return
	}

	scrollID = r["_scroll_id"].(string)
	hits = r["hits"].(map[string]interface{})["hits"].([]interface{})
	return
}
