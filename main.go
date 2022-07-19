package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

func main() {
	es, err := NewES([]string{"http://localhost:9200"})
	if err != nil {
		log.Fatal(err)
	}
	res, err := es.Info()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res)

	zinc, err := NewZinc("http://localhost:4080", "admin", "Complexpass#123")
	if err != nil {
		log.Fatal(err)
	}
	version, err := zinc.Version()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(version)

	scrollID, total, hits, err := es.Search("olympics", []byte(`{"query": {"match_all": {}}}`))
	if err != nil {
		log.Fatal(err)
	}
	n := len(hits)
	fmt.Println(scrollID, total, len(hits), n)
	fmt.Printf("%s\n", strings.Repeat("=", 36))
	for _, hit := range hits {
		hit := hit.(map[string]interface{})
		source := hit["_source"].(map[string]interface{})
		body, err := json.Marshal(source)
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("%s\n", body)
		}
		id, err := zinc.IndexDocument("olympics", source)
		fmt.Println("zinc, id", id, err)
	}

	fmt.Printf("%s\n", strings.Repeat("=", 36))

	// for {
	// 	scrollID, hits, err = es.Scroll(scrollID)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	if len(hits) == 0 {
	// 		break
	// 	}
	// 	n += len(hits)
	// 	fmt.Println(scrollID, total, len(hits), n)
	// }
}
