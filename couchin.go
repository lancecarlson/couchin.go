package main

import (
	"log"
//	"fmt"
	"io/ioutil"
	"flag"
	"github.com/vmihailenco/redis"
	"encoding/base64"
	"net/http"
	"strings"
//	"time"
)

func Partition(list []string, size int, f func([]string)) {
	for i := 0; i < len(list); i += size {
		if i+size > len(list) {
			f(list[i:])
		} else {
			f(list[i:i+size])
		}
	}
}

func Decode(val string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(val)
}

func GetDocs(keys []string, client *redis.Client) []string {
	docs := make([]string, len(keys))
	for i, key := range keys {
		get := client.Get(key)
		if get.Err() != nil {
			log.Fatal(get.Err())
		} else {
			doc, err := Decode(get.Val())
			if err != nil {
				log.Fatal(err)
			} else {
				docs[i] = string(doc)
			}
		}
	}
	return docs
}

func SaveDocs(docs []string, saveUrl string) (*http.Response, error) {
	body := `{"docs":[`
	body += strings.Join(docs, ",")
	body += `]}`
	return http.Post(saveUrl, "application/json", strings.NewReader(body))
}

func Work(keys []string, client *redis.Client, saveUrl *string) (string, error) {
	docs := GetDocs(keys, client)
	resp, err := SaveDocs(docs, *saveUrl)
	if err != nil { return "", err }
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil { return "", err }
	return string(body), nil
}

func Worker(id int, client *redis.Client, saveUrl *string, jobs <- chan []string, results chan<- string) {
	for j := range jobs {
		log.Println("worker", id, "processing job")
		log.Println(len(j))
		body, err := Work(j, client, saveUrl)
		if err != nil {
			log.Println(err)
			results <- ""
		} else {
			log.Println(body)
			results <- body
		}
	}
}

func main() {
	password := flag.String("password", "", "password for Redis")
	host := flag.String("host", "localhost:6379", "host for Redis")
	db := flag.Int64("db", 0, "select the Redis db integer")
	saveUrl := flag.String("save-url", "", "bulk save url. ie: http://localhost:5984/db/_bulk_docs")
	saveLimit := flag.Int("save-limit", 1000, "number of docs to save at once")
	workerCount := flag.Int("workers", 20, "number of works to batch save")
	flush := flag.Bool("flush", false, "flush Redis after finished")
	flag.Parse()
	client := redis.NewTCPClient(*host, *password, *db)
	defer client.Close()
	
	keys := client.Keys("*")

	if keys.Err() != nil {
		log.Fatal(keys.Err())
	} else {
		keyCount := len(keys.Val())
		jobCount := int(keyCount/(*saveLimit))+1
		if keyCount <= 0 {
			log.Fatal("No Keys")
		}
		jobs := make(chan []string, jobCount)
		results := make(chan string, jobCount)

		for w := 1; w <= *workerCount; w++ {
			go Worker(w, client, saveUrl, jobs, results)
		}

		Partition(keys.Val(), *saveLimit, func(keys []string) {
			jobs <- keys
		})

		close(jobs)

		for a := 1; a <= jobCount; a++ {
			<-results
		}

		if *flush {
			log.Println("Flushing...")
			client.FlushDb()
		}
	}
}