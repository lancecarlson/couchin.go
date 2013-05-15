package main

import (
	"os"
	"log"
	"fmt"
	"io/ioutil"
	"flag"
	"github.com/vmihailenco/redis"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"errors"
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

func GetDoc(key string, client *redis.Client) (string, error) {
	if key[0:1] == "_" { return "", errors.New("reserved key: " + key) }
	get := client.Get(key)
	if get.Err() != nil { return "", get.Err() }
	doc, err := Decode(get.Val())
	if err != nil { return "", err }
	return string(doc), nil
}

func GetDocs(keys []string, client *redis.Client) []string {
	docs := make([]string, len(keys))
	for i, key := range keys {
		doc, err := GetDoc(key, client)
		if err == nil {
			docs[i] = doc
		} else {
			log.Print("Err: ")
			log.Println(err)
		}
	}
	return docs
}

func BuildDocsBody(docs []string) string {
	body := `{"docs":[`
	body += strings.Join(docs, ",")
	body += `]}`
	return body
}

func SaveDocs(docs []string, saveUrl string) (*http.Response, error) {
	return http.Post(saveUrl, "application/json", strings.NewReader(BuildDocsBody(docs)))
}

type Response struct {
	Ok bool
	Id string
	Rev string
	Error string
	Reason string
}

func Work(keys []string, client *redis.Client, saveUrl string, printRequest bool) (body []byte, docResps []Response, docs []string, err error) {
	docs = GetDocs(keys, client)
	if printRequest { fmt.Println(BuildDocsBody(docs)) }
	resp, err := SaveDocs(docs, saveUrl)
	if err != nil { return nil, nil, nil, err }
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil { return nil, nil, nil, err }
	err = json.Unmarshal(body, &docResps)
	if err != nil { return nil, nil, nil, err }
	return body, docResps, docs, nil
}

func Worker(id int, client *redis.Client, saveUrl string, printRequest bool, printResults string, printStatus bool, jobs <- chan []string, results chan<- []Response) {
	for j := range jobs {
		if printStatus {
			log.Println("worker", id, "processing job")
			log.Println(len(j))
		}

		rawResp, body, docs, err := Work(j, client, saveUrl, printRequest)
		if err != nil {
			log.Println(err)
			results <- []Response{}
		} else {
			if printResults == "all" {
				for _, resp := range body {
					fmt.Println(resp)
				}
			} else if printResults == "error" {
				for _, resp := range body {
					if !resp.Ok {
						fmt.Println(resp)
					}
				}
			} else if printResults == "erroranddoc" {
				for index, resp := range body {
					if !resp.Ok {
						fmt.Println(resp)
						fmt.Println(docs[index])
					}
				}
			} else if printResults == "raw" {
				fmt.Println(string(rawResp))
			}
			results <- body
		}
	}
}

func main() {
	password := flag.String("password", "", "password for Redis")
	host := flag.String("host", "localhost:6379", "host for Redis")
	db := flag.Int64("db", 0, "select the Redis db integer")
	saveLimit := flag.Int("save-limit", 100, "number of docs to save at once")
	workerCount := flag.Int("workers", 20, "number of workers to batch save")
	printRequest := flag.Bool("print-request", false, "output the request body")
	printResults := flag.String("print-results", "", "output the result of each bulk request. (all|error|erroranddoc|raw)")
	printStatus := flag.Bool("print-status", false, "output the result the status of workers")
	flush := flag.Bool("flush", true, "flush Redis after finished")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s [options] [save url]:\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	saveUrl := flag.Arg(0)
	if saveUrl == "" {
		flag.Usage()
		log.Fatal("missing bulk save url as first argument. ie: http://localhost:5984/db/_bulk_docs")
	}

	log.Println("Save Limit: ", *saveLimit)
	log.Println("Worker Count: ", *workerCount)
	log.Println("Flush: ", *flush)

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
		results := make(chan []Response, jobCount)

		for w := 1; w <= *workerCount; w++ {
			go Worker(w, client, saveUrl, *printRequest, *printResults, *printStatus, jobs, results)
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

		log.Println("Done.")
	}
}