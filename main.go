package main

import (
	"fmt"
	"log"
	"strings"
)

func main() {
	// init ES
	es, err := NewES([]string{Config.ESHost}, Config.ESUser, Config.ESPassword)
	if err != nil {
		log.Fatal(err)
	}
	res, err := es.Info()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res)
	fmt.Printf("%s\n", strings.Repeat("=", 36))

	// init Zinc
	zinc, err := NewZinc(Config.ZincHost, Config.ZincUser, Config.ZincPassword)
	if err != nil {
		log.Fatal(err)
	}
	version, err := zinc.Version()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(version)
	fmt.Printf("%s\n", strings.Repeat("=", 36))

	// read from ES
	scrollID, total, hits, err := es.Search(Config.ESIndexName, []byte(`{"query": {"match_all": {}}}`))
	if err != nil {
		log.Fatal(err)
	}
	n := len(hits)
	fmt.Println(total, len(hits), n)
	for _, hit := range hits {
		hit := hit.(map[string]interface{})
		source := hit["_source"].(map[string]interface{})
		// body, err := json.Marshal(source)
		// if err != nil {
		// 	log.Fatal(err)
		// } else {
		// 	fmt.Printf("%s\n", body)
		// }

		// write to Zinc
		id, err := zinc.IndexDocument(Config.ZincIndexName, source)
		fmt.Println("zinc, id", id, err)
	}

	for {
		scrollID, hits, err = es.Scroll(scrollID)
		if err != nil {
			log.Fatal(err)
		}
		if len(hits) == 0 {
			break
		}
		n += len(hits)
		fmt.Println(total, len(hits), n)
		for _, hit := range hits {
			hit := hit.(map[string]interface{})
			source := hit["_source"].(map[string]interface{})
			// body, err := json.Marshal(source)
			// if err != nil {
			// 	log.Fatal(err)
			// } else {
			// 	fmt.Printf("%s\n", body)
			// }

			// write to Zinc
			id, err := zinc.IndexDocument(Config.ZincIndexName, source)
			fmt.Println("zinc, id", id, err)
		}
	}
}
