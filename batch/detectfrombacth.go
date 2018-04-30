package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	grpc "github.com/micro/go-grpc"
	micro "github.com/micro/go-micro"
	"github.com/olivere/elastic"
	proto "github.com/plagiari-sm/svc-proto/plagiarism"
)

// Document ..
type Document struct {
	Lang       string `json:"lang"`
	CrawledAt  string `json:"crawledAt"`
	ScreenName string `json:"screenName"`
	URL        string `json:"url"`
	TweetID    uint64 `json:"tweetId"`
	Content    struct {
		Title       string   `json:"title,omitempty"`
		Excerpt     string   `json:"excerpt,omitempty"`
		Body        string   `json:"body,omitempty"`
		Authors     []string `json:"authors,omitempty"`
		Sources     []string `json:"sources,omitempty"`
		Tags        []string `json:"tags,omitempty"`
		Categories  []string `json:"categories,omitempty"`
		PublishedAt string   `json:"publishedAt,omitempty"`
		EditedAt    string   `json:"editedAt,omitempty"`
	} `json:"content"`
	NLP struct {
		Tokens    []string `json:"tokens,omitempty"`
		StopWords []string `json:"stopWords,omitempty"`
	} `json:"-"`
}

func main() {
	// Create a new service. Optionally include some options here.
	service := grpc.NewService(micro.Name("plagiari-sm-grpc-plagiarism-batch"))

	// Create new greeter client
	p := proto.NewPlagiarismClient("plagiari-sm-grpc-plagiarism", service.Client())
	fmt.Println(p)
	// Print response
	// Setup Elasticsearch
	esclient, err := elastic.NewClient(
		elastic.SetURL("http://localhost:9200"),
		elastic.SetBasicAuth("", ""),
		elastic.SetHealthcheckTimeoutStartup(15*time.Second),
	)
	if err != nil {
		log.Fatalf("[PSM-UTILS] Elasticsearch Connection Error: %v", err)
	}

	// Build the Query
	// New Bool Query
	query := elastic.NewBoolQuery()
	total, _ := esclient.Count("articles").Type("document").Query(query).Do(context.Background())
	if total == 0 {
		// log.Infof("[SVC-PLAGIARISM] Non Similar, Skipping.")
		return
	}

	searchResult, err := esclient.Search().
		Index("articles").
		Query(query).
		Sort("crawledAt", true).
		Pretty(true).
		From(0).Size(10000).
		Do(context.Background())
	if err != nil {
		panic(err)
	}

	// Here's how you iterate through results with full control over each step.
	if searchResult.Hits.TotalHits > 0 {
		fmt.Printf("Found a total of %d tweets\n", searchResult.Hits.TotalHits)

		// Iterate through results
		for _, hit := range searchResult.Hits.Hits {
			// hit.Index contains the name of the index

			// Work with tweet
			// Call the greeter
			_, err := p.Detect(context.TODO(), &proto.RequestDocument{
				Id: hit.Id,
			})
			if err != nil {
				fmt.Println(err)
			}
			var t Document
			err = json.Unmarshal(*hit.Source, &t)
			if err != nil {
				// Deserialization failed
			}
			fmt.Printf("ID: %s \t %s\n", hit.Id, t.CrawledAt)
			time.Sleep(250 * time.Millisecond)
		}
	} else {
		// No hits
		fmt.Print("Found no tweets\n")
	}
	/*
		// Init Go Routines for Search Scroller
		hits := make(chan json.RawMessage)
		g, ctx := errgroup.WithContext(context.Background())
		g.Go(func() error {
			defer close(hits)
			// Scroller
			scroll := esclient.Scroll("articles").Type("document").Query(query).Size(100)

			// Itterate
			for {
				results, err := scroll.Do(context.Background())
				if err == io.EOF {
					return nil // all results retrieved
				}
				if err != nil {
					return err // something went wrong
				}
				// Send the hits to the hits channel
				for _, hit := range results.Hits.Hits {
					select {
					case hits <- *hit.Source:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		})

		// Init Go Routines for Search Results
		for i := 0; i < 10; i++ {
			g.Go(func() error {
				for hit := range hits {
					fmt.Print(hit)
					// Terminate
					select {
					default:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			})
		}

		// Check whether any goroutines failed.
		if err := g.Wait(); err != nil {
			log.Print(err)
		}
	*/
}
