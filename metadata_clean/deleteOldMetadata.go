// Package metadata_clean delete expire metadata
package main

import (
	"fmt"
	"log"
	"moss/pkg/es"
)

const MIN_VERSION_COUNT = 5

func main() {
	buckets, err := es.SearchVersionStatus(MIN_VERSION_COUNT)
	if err != nil {
		log.Println("deleteOldMetadata() error:", err)
		return
	}
	fmt.Println(buckets)
	for i := range buckets {
		bucket := buckets[i]
		for v := 0; v < bucket.Doc_count-MIN_VERSION_COUNT; v++ {
			es.DelMetadata(bucket.Key, v+int(bucket.Min_version.Value))
		}
	}
}
