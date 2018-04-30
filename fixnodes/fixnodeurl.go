package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/apex/log"
	"github.com/jmcvetta/neoism"
	"github.com/olivere/elastic"
	"github.com/plagiari-sm/psm-utils/utils"
	"golang.org/x/sync/errgroup"
)

var (
	es  *elastic.Client
	neo *neoism.Database
)

// Document ..
type Document struct {
	DocID      string `json:"docId"`
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
	} `json:"nlp"`
}

func main() {
	esurl := fmt.Sprintf(
		"http://%s:%s",
		utils.GetEnv("ES_HOST", "127.0.0.1"),
		utils.GetEnv("ES_PORT", "9200"),
	)
	esclient, err := elastic.NewClient(
		elastic.SetURL(esurl),
		elastic.SetBasicAuth(utils.GetEnv("ES_USER", ""), utils.GetEnv("ES_PASS", "")),
		elastic.SetHealthcheckTimeoutStartup(15*time.Second),
	)
	if err != nil {
		log.Fatalf("[DB] Elasticsearch Connection Error: %v", err)
	}
	// Assign client to es pointer
	es = esclient

	neoclient, err := neoism.Connect(utils.GetEnv(
		"NEO_URL", "http://neo4j:123@localhost:7474/db/data/",
	))
	if err != nil {
		log.Fatalf("[DB] Noe4j Connection Error: %v", err)
	}
	neo = neoclient

	// Build the Query
	// New Bool Query
	query := elastic.NewBoolQuery()
	total, _ := es.Count("articles").Type("document").Query(query).Do(context.Background())
	if total == 0 {
		// log.Infof("[SVC-PLAGIARISM] Non Similar, Skipping.")
		return
	}
	// Init Go Routines for Search Scroller
	hits := make(chan json.RawMessage)
	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		defer close(hits)
		// Scroller
		scroll := es.Scroll("articles").Type("document").Query(query).Size(100)

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
				// Deserialize
				var d Document
				err := json.Unmarshal(hit, &d)
				if err != nil {
					return err
				}

				updateNode(d.TweetID, d.URL)

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
	fmt.Println("Done.")
	// Check whether any goroutines failed.
	if err := g.Wait(); err != nil {
		log.Fatalf(err.Error())
	}
}

func updateNode(id uint64, url string) {
	c := neoism.CypherQuery{
		Statement:  `MATCH (a:Article) WHERE a.tweetId = {id} SET a.url = {url}`,
		Parameters: neoism.Props{"id": id, "url": url},
	}

	err := neo.Cypher(&c)
	if err != nil {
		panic(err)
	}
}
