package es

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"moss/pkg/setting"
	"net/http"
	url2 "net/url"
	"strings"
)

type Metadata struct {
	Name    string
	Hash    string
	Version int
	Size    int64
}

type hit struct {
	Source Metadata `json:"_source"`
}

type SearchResult struct {
	Hits struct {
		Total int
		Hits  []hit
	}
}

func getMetadata(name string, version int) (meta Metadata, err error) {
	url := fmt.Sprintf("%s/metadata/_doc/%s_%d/_source", setting.ES, name, version)
	//log.Println("getMetadata url: ", url)
	resp, err := http.Get(url)
	if err != nil {
		return
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("getMetadata: HTTP status code %d", resp.StatusCode)
		return
	}

	result, _ := io.ReadAll(resp.Body)
	err = json.Unmarshal(result, &meta)
	if err != nil {
		log.Printf("getMetadata: json unmarshal err %v", err)
	}
	return
}

func SearchLatestVersion(name string) (meta Metadata, err error) {
	url := fmt.Sprintf("%s/metadata/_search?q=name:%s&size=1&sort=version:desc", setting.ES, url2.PathEscape(name))
	log.Println("SearchLatestVersion url: ", url)
	resp, err := http.Get(url)
	if err != nil {
		return
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("SearchLatestVersion: HTTP status code %d\n", resp.StatusCode)
		return
	}
	result, _ := io.ReadAll(resp.Body)
	var searchResult SearchResult
	json.Unmarshal(result, &searchResult)
	//log.Println("SearchLatestVersion result: ", result)
	if len(searchResult.Hits.Hits) > 0 {
		meta = searchResult.Hits.Hits[0].Source
	}
	return
}

func GetMetadata(name string, version int) (meta Metadata, err error) {
	if version == 0 {
		return SearchLatestVersion(name)
	}
	return getMetadata(name, version)
}

func PutMetadata(name string, version int, size int64, hash string) (err error) {
	doc := fmt.Sprintf(`{"name":"%s","version":%d,"size":%d,"hash":"%s"}`, name, version, size, hash)
	url := fmt.Sprintf("%s/metadata/_doc/%s_%d", setting.ES, name, version)
	//log.Println("PutMetadata url: ", url)
	//log.Println("PutMetadata doc: ", doc)
	client := http.Client{}
	request, _ := http.NewRequest(http.MethodPut, url, strings.NewReader(doc))
	request.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(request)
	if err != nil {
		return err
	}

	//log.Println("PutMetadata resp: ", resp)
	if resp.StatusCode == http.StatusConflict {
		return PutMetadata(name, version+1, size, hash)
	}

	if resp.StatusCode != http.StatusCreated {
		result, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PutMetadata error: %d %s", resp.StatusCode, result)
	}
	return nil
}

func AddVersion(name string, size int64, hash string) (err error) {
	metadata, err := SearchLatestVersion(name)
	if err != nil {
		return err
	}

	return PutMetadata(name, metadata.Version+1, size, hash)
}

// SearchAllVersions search object name from 'from' and at most size
func SearchAllVersions(name string, from, size int) ([]Metadata, error) {
	//log.Println("SearchAllVersions params: ", name, from, size)
	url := fmt.Sprintf("%s/metadata/_search?sort=name.keyword,version&from=%d&size=%d", setting.ES, from, size)
	if len(name) > 0 {
		url += "&q=name:" + name
	}
	//log.Println("SearchAllVersions url: ", url)
	r, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	result, _ := io.ReadAll(r.Body)
	var searchResult SearchResult
	json.Unmarshal(result, &searchResult)
	metadatas := make([]Metadata, len(searchResult.Hits.Hits))
	for i := range searchResult.Hits.Hits {
		metadatas[i] = searchResult.Hits.Hits[i].Source
	}
	return metadatas, nil
}

type Bucket struct {
	Key         string
	Doc_count   int `json:"doc_count"`
	Min_version struct {
		Value float32
	} `json:"min_version"`
}

type aggregateResult struct {
	Aggregations struct {
		Group_by_name struct {
			Buckets []Bucket `json:"buckets"`
		} `json:"group_by_name"`
	} `json:"aggregations"`
}

func SearchVersionStatus(count int) ([]Bucket, error) {
	client := http.Client{}
	url := fmt.Sprintf("%s/metadata/_search", setting.ES)
	body := fmt.Sprintf(`
        {
         "size": 0,
         "aggs": {
           "group_by_name": {
             "terms": {
               "field": "name.keyword",
               "min_doc_count": %d
             },
             "aggs": {
               "min_version": {
                 "min": {
                   "field": "version"
                 }
               }
             }
           }
         }
        }`, count)
	request, _ := http.NewRequest("GET", url, strings.NewReader(body))
	request.Header.Set("content-type", "application/json")
	resp, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	b, _ := io.ReadAll(resp.Body)
	var result aggregateResult
	err = json.Unmarshal(b, &result)
	if err != nil {
		return nil, err
	}
	fmt.Println(string(b))
	return result.Aggregations.Group_by_name.Buckets, nil
}

func DelMetadata(name string, version int) {
	client := http.Client{}
	url := fmt.Sprintf("%s/metadata/_doc/%s_%d", setting.ES, name, version)
	fmt.Println("delete", url)
	request, _ := http.NewRequest("DELETE", url, nil)
	_, err := client.Do(request)
	if err != nil {
		log.Println(err)
	}
}

func HasHash(hash string) (bool, error) {
	url := fmt.Sprintf("%s/metadata/_search?q=hash:%s&size=0", setting.ES, hash)
	resp, err := http.Get(url)
	if err != nil {
		return false, err
	}
	b, _ := io.ReadAll(resp.Body)
	var searchResult SearchResult
	json.Unmarshal(b, &searchResult)
	return searchResult.Hits.Total != 0, nil
}

func SearchHashSize(hash string) (size int64, err error) {
	url := fmt.Sprintf("%s/metadata/_search?q=hash:%s&size=1", setting.ES, hash)
	r, err := http.Get(url)
	if err != nil {
		return
	}
	if r.StatusCode != http.StatusOK {
		err = fmt.Errorf("fail to search hash size, status code: %d", r.StatusCode)
		return
	}
	result, _ := io.ReadAll(r.Body)
	var searchResult SearchResult
	json.Unmarshal(result, &searchResult)
	if len(searchResult.Hits.Hits) > 0 {
		size = searchResult.Hits.Hits[0].Source.Size
	}
	return
}
