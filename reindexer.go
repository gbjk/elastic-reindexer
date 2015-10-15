/* Work in progress reindexer

   TODO:
    * Walk back through indexes
    * Split indexes out
    * Modular transformation of existing records
*/
package main

import (
	"fmt"
	"gopkg.in/olivere/elastic.v2"
	"log"
	"time"
)

func main() {
	client, err := elastic.NewClient(elastic.SetURL("http://log01.live.thermeon.eu:9200"))
	if err != nil {
		log.Fatal("Error Connecting to elastic: ", err)
	}

	indexName := "logstash-2015.10.13"

	indexResp, err := client.IndexGet().Index(indexName).Do()
	if err != nil {
		log.Fatal("Couldn't get information about index")
	}

	indexBody := indexResp[indexName]
	webxgEventsInterface := indexBody.Mappings["webxg_event"]
	webxgEvents := webxgEventsInterface.(map[string]interface{})
	webxgProps := webxgEvents["properties"].(map[string]interface{})
	webxgMetricProps := webxgProps["metrics"].(map[string]interface{})
	webxgMetrics := webxgMetricProps["properties"].(map[string]interface{})

	for k := range webxgMetrics {
		webxgMetrics[k] = map[string]string{
			"type": "double",
		}
	}

	newIndexName := indexName + "_v2"

	createIndex, err := client.CreateIndex(newIndexName).BodyJson(indexBody).Do()

	if err != nil {
		log.Fatal("Error creating index: ", err)
	}
	if !createIndex.Acknowledged {
		log.Fatal("Create index not acknowledged")
	}

	reindexer := client.Reindex(indexName, newIndexName)
	reindexer.BulkSize(1000)
	started := time.Now()
	reindexer.Progress(func(current, total int64) {
		if current%10000 == 0 {
			taken := time.Since(started)
			percent := float64(current) / float64(total) * 100
			fmt.Printf("%d %%: %d of %d copied in %d minutes %d seconds \n", int(percent), current, total, int(taken.Minutes()), int(taken.Seconds()))
		}
	})
	result, err := reindexer.Do()
	if err != nil {
		log.Fatal("Reindex failed: ", err)
	}
	fmt.Println(result)

	fmt.Println("Done")
}
