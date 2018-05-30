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

type cus struct {
	DocID       string `json:"docId"`
	TweetID     uint64 `json:"tweetId"`
	ScreenName  string `json:"screenName"`
	PublishedAt string `json:"publishedAt,omitempty"`
}
type rel struct {
	Source cus     `json:"source"`
	Target cus     `json:"target"`
	Score  float64 `json:"score"`
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
	// Range Source.PublishedAt -1 Week
	query = query.Must(
		elastic.NewRangeQuery("source.publishedAt").
			Gte("now-1d"),
	)

	total, _ := es.Count("relationships").Type("document").Query(query).Do(context.Background())
	if total == 0 {
		return
	}

	log.Errorf("[relationships]: %d", total)

	// Init Go Routines for Search Scroller
	hits := make(chan json.RawMessage)
	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		defer close(hits)
		// Scroller
		scroll := es.Scroll("relationships").Type("document").Query(query).Size(100)

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
				var d rel
				err := json.Unmarshal(hit, &d)
				if err != nil {
					return err
				}
				fmt.Println(d)

				s, err := es.Get().Index("articles").Type("document").Id(d.Source.DocID).Do(context.Background())
				if err != nil {
					return err
				}

				t, err := es.Get().Index("articles").Type("document").Id(d.Target.DocID).Do(context.Background())
				if err != nil {
					return err
				}

				var a Document
				err = json.Unmarshal(*s.Source, &a)
				if err != nil {
					return err
				}

				var b Document
				err = json.Unmarshal(*t.Source, &b)
				if err != nil {
					return err
				}

				SaveRelation(&a, &b, d.Score)

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
		return
	}
}

// SaveRelation Save Relationship to Neo4j
func SaveRelation(a, b *Document, score float64) {
	tsource, _ := time.Parse(time.RFC3339, a.Content.PublishedAt)
	source, _, err := neo.GetOrCreateNode("Article", "tweetId", neoism.Props{
		"title":       a.Content.Title,
		"screenName":  a.ScreenName,
		"publishedAt": a.Content.PublishedAt,
		"tweetId":     a.TweetID,
		"docId":       a.DocID,
		"url":         a.URL,
	})
	if err != nil {
		log.Errorf("[SVC-PLAGIARISM] Can't Create Node: %s", a.Content.Title)
		return
	}
	defer source.Delete()
	source.AddLabel("Article")

	ttarget, _ := time.Parse(time.RFC3339, b.Content.PublishedAt)
	target, _, err := neo.GetOrCreateNode("Article", "tweetId", neoism.Props{
		"title":       b.Content.Title,
		"screenName":  b.ScreenName,
		"publishedAt": b.Content.PublishedAt,
		"tweetId":     b.TweetID,
		"docId":       b.DocID,
		"url":         a.URL,
	})
	if err != nil {
		log.Errorf("[SVC-PLAGIARISM] Can't Create Node: %s", b.Content.Title)
		return
	}
	defer target.Delete()
	target.AddLabel("Article")
	// log.Infof("Source: %v, Target: %v, DIFF: %v", tsource, ttarget, tsource.Sub(ttarget).Minutes())
	if tsource.Sub(ttarget).Minutes() <= 0 {
		source.Relate("RELATED_WITH", target.Id(), neoism.Props{"score": score})
	} else {
		target.Relate("RELATED_WITH", source.Id(), neoism.Props{"score": score})
	}
}
