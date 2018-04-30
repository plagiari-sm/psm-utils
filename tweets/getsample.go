package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ChimeraCoder/anaconda"
	"github.com/apex/log"
	feeds "github.com/plagiari-sm/api-feeds/models"
	"github.com/plagiari-sm/psm-utils/utils"
	scrape "github.com/plagiari-sm/svc-proto/scrape"
	"google.golang.org/grpc"
)

var (
	api    *anaconda.TwitterApi
	svc    *grpc.ClientConn
	method string
	file   string
	since  string
)

// Tweet Data for CSV
type Tweet struct {
	Order      int64
	CreatedAt  string
	ScreenName string
	ID         string
	URL        string
}

func init() {
	flag.StringVar(&method, "method", "crawl", "Method Name")
	flag.StringVar(&file, "file", "", "Data File to Import")
	flag.StringVar(&since, "since", "", "Since ID (Twitter)")
	flag.Parse()
}

func main() {
	if method == "crawl" {
		crawl()
	}

	if method == "save" {
		save()
	}

	if method == "read" {
		read()
	}
}
func read() {
	csvFile, _ := os.Open(file)
	r := csv.NewReader(bufio.NewReader(csvFile))

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		fmt.Println(record)
	}
}
func save() {
	svc, err := grpc.Dial(
		utils.GetEnv("SVC_SCRAPE", "localhost:50050"),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithTimeout(15*time.Second),
	)
	if err != nil {
		fmt.Printf("[SVC-SCRAPE] GRPC Srape did not connect: %v", err)
		panic(err)
	}
	fs := getFeeds()
	csvFile, _ := os.Open(file)
	r := csv.NewReader(bufio.NewReader(csvFile))
	I := 0
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		// order record[0]
		// createdAt record[1]
		// screenName record[2]
		// id record[3]
		// url record[4]
		if feed := pickFeed(strings.ToLower(record[2]), fs); feed != nil {
			f := map[string]string{
				"title":       feed.MetaClasses.Title,
				"excerpt":     feed.MetaClasses.Excerpt,
				"body":        feed.MetaClasses.Body,
				"authors":     feed.MetaClasses.Authors,
				"sources":     feed.MetaClasses.Sources,
				"tags":        feed.MetaClasses.Tags,
				"categories":  feed.MetaClasses.Categories,
				"publishedAt": feed.MetaClasses.PublishedAt,
				"editedAt":    feed.MetaClasses.EditedAt,
			}
			if feed.MetaClasses.FeedType == "js" {
				f["api"] = feed.MetaClasses.API
			}

			jsonString, _ := json.Marshal(f)
			client := scrape.NewGRPCScrapeClient(svc)
			id, _ := strconv.ParseInt(record[3], 10, 64)
			fmt.Printf("%d %s Scraping Article from: %s\n", I, record[1], feed.ScreenName)
			go client.Scrape(context.Background(), &scrape.RequestStream{
				Feed:       string(jsonString),
				Url:        record[4],
				TweetId:    id,
				Lang:       feed.Lang,
				ScreenName: feed.ScreenName,
				CrawledAt:  record[1],
			})
		}
		I++
		time.Sleep(600 * time.Millisecond)
	}
}

func crawl() {
	api = anaconda.NewTwitterApiWithCredentials(
		utils.GetEnv("TWITTER_ACCESS_TOKEN", "44142397-GhgFILxreXLa0vdCRMdEY0LRgH4qd1MhMGWJfR0W4"),
		utils.GetEnv("TWITTER_ACCESS_TOKEN_SECRET", "anRvEBfGVEgaLHgrlfyLikjQMJALBAJzx5N1v7zDp3sDS"),
		utils.GetEnv("TWITTER_CONSUMER_KEY", "HD7z3hIrvRNShStXhIdBSfiZB"),
		utils.GetEnv("TWITTER_CONSUMER_SECRET", "GsQeLf7aYgTeDY5S46sqn09a4I6JGZSpGf8JkOx3DYd7zBRco3"),
	)
	t := time.Now()
	file, err := os.Create(fmt.Sprintf("data-%s.csv", t.Format("2006-01-02")))
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	fmt.Printf("Start Crawling, Please Wait...")
	fs := getFeeds()
	for f := range fs {
		getTweets(fs[f].ScreenName, 0, "", *writer, since)
	}

	fmt.Printf("Done.\n")
}

func getTweets(s string, n int, id string, writer csv.Writer, opts ...string) bool {
	v := url.Values{}
	v.Set("count", "200")
	if id != "" {
		v.Set("max_id", id)
	}
	v.Set("screen_name", s)
	if len(opts) > 0 && opts[0] != "" {
		v.Set("since_id", opts[0])
	}

	tweets, err := api.GetUserTimeline(v)
	if err != nil {
		fmt.Println(err)
		time.Sleep(15 * time.Minute)
		return getTweets(s, n, id, writer)
	}
	for _, t := range tweets {
		if isTweet(t) {
			if len(t.Entities.Urls) == 0 {
				return false
			}

			for u := range t.Entities.Urls {
				// Introduce clean URL logic
				// Remove Twitter Share ID (i.e. /#.WpAW30E8tRc.twitter)
				link := regexp.MustCompile(`\#..*$`).ReplaceAllString(t.Entities.Urls[u].Expanded_url, `$1`)
				link = regexp.MustCompile(`\?utm_source.*$`).ReplaceAllString(link, `$1`)

				// Test URL
				p, err := url.Parse(link)
				// Don't allow Root Domain or Error
				if len(p.Path) <= 1 || err != nil {
					continue
				}
				order, _ := t.CreatedAtTime()
				writer.Write([]string{
					strconv.FormatInt(order.Unix(), 10),
					t.CreatedAt,
					t.User.ScreenName,
					strconv.FormatInt(t.Id, 10),
					link,
				})
			}
		}
	}

	n++
	if n*200 < 3200 {
		return getTweets(s, n, tweets[len(tweets)-1].IdStr, writer)
	}
	return true
}

// getFeeds returns the active feeds
func getFeeds() []*feeds.Feed {
	resp, err := http.Get(utils.GetEnv("API_FEEDS", "http://localhost:8000") + "/api/v1/feeds/?status=active&lang=EL")
	if err != nil {
		log.Errorf("[SVC-LISTEN] Feeds loading error: %v", err)
		panic(err)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("[SVC-LISTEN] Feeds parsing error: %v", err)
		panic(err)
	}

	fs := &feeds.Feeds{}
	json.Unmarshal(body, &fs)
	return fs.Data
}

func pickFeed(s string, fs []*feeds.Feed) *feeds.Feed {
	for f := range fs {
		if s == fs[f].ScreenName {
			return fs[f]
		}
	}

	return nil
}
func isTweet(t anaconda.Tweet) bool {
	if t.InReplyToStatusIdStr == "" && t.InReplyToUserIdStr == "" &&
		t.RetweetedStatus == nil && t.QuotedStatus == nil {
		return true
	}
	return false
}
