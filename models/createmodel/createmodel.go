package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/olivere/elastic"
	"github.com/plagiari-sm/psm-utils/utils"
	"golang.org/x/sync/errgroup"
)

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

	// Build the Query
	// New Bool Query
	query := elastic.NewBoolQuery()
	total, _ := esclient.Count("articles").Type("document").Query(query).Do(context.Background())
	if total == 0 {
		return
	}
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
				// Deserialize
				var d struct {
					Content struct {
						Body string `json:"body,omitempty"`
					} `json:"content"`
				}
				err := json.Unmarshal(hit, &d)
				if err != nil {
					return err
				}

				fmt.Println(preCleanText(d.Content.Body))
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
		panic(err)
	}
}

// preCleanText : Converts string to lowercase and remove \t, \n chars
func preCleanText(str string) string {
	// Return if empty
	if len(str) < 0 {
		return ""
	}

	// Regexp for leading space (\r), new line (\n) and tab (\t)
	re := regexp.MustCompile("(\r)?(\n)?(\t)")

	// Convert all chars to Uppercase
	// Uppercase is prefered against Lowercase because on some langeuages (ie. Greek) lowercase ending tokens (ie. σ, ς) are different from the uppercase
	// But yet again languages like Greek have accents
	// Need more testing here
	output := str

	output = strings.Trim(output, " ")

	// Remove Regexp
	output = re.ReplaceAllString(output, " ")

	// Remove single quote, this is important
	// Greek single quote will not split
	output = strings.Replace(output, "'", "", -1)

	output = strings.Replace(output, "\n", "", -1)

	// Then replace line breaks with newlines, to preserve that formatting
	output = strings.Replace(output, "</p>", "\n", -1)
	output = strings.Replace(output, "<br>", "\n", -1)
	output = strings.Replace(output, "</br>", "\n", -1)
	output = strings.Replace(output, "<br/>", "\n", -1)
	output = strings.Replace(output, "<br />", "\n", -1)

	// Remove a few common harmless entities, to arrive at something more like plain text
	output = strings.Replace(output, "&#8216;", "'", -1)
	output = strings.Replace(output, "&#8217;", "'", -1)
	output = strings.Replace(output, "&#8220;", "\"", -1)
	output = strings.Replace(output, "&#8221;", "\"", -1)
	output = strings.Replace(output, "&nbsp;", " ", -1)
	output = strings.Replace(output, "&quot;", "\"", -1)
	output = strings.Replace(output, "&apos;", "'", -1)

	// After processing, remove some harmless entities &, ' and " which are encoded by HTMLEscapeString
	output = strings.Replace(output, "&#34;", "\"", -1)
	output = strings.Replace(output, "&#39;", "'", -1)
	output = strings.Replace(output, "&amp; ", "& ", -1)     // NB space after
	output = strings.Replace(output, "&amp;amp; ", "& ", -1) // NB space after

	// TODO
	// Remove links
	// Remove quoted text and pointer if exists (ie. βόλφγκανγκ σόιμπλε: «ο αριθμός των υποστηρικτών μιας ελάφρυνσης χρέους για την ελλάδα αυξάνεται, αφότου η αθήνα επικύρωσε ένα νέο μεταρρυθμιστικό πακέτο. ωστόσο, κάποιος δεν θέλει ακόμη να πάει μαζί τους»)
	// Remove HTML

	// Retrun cleaned text
	return "\"" + output + "\""
}
