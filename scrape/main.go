package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"gopkg.in/urfave/cli.v2"
)

func main() {
	app := &cli.App{
		Name:  "scrape",
		Usage: "fetch and parse cards",
		Commands: []*cli.Command{
			{
				Name: "urls",
				Action: func(c *cli.Context) error {
					return WriteAllCardURLs(c.String("path"))
				},
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "path", Value: "card-urls.txt"},
				},
			},
			{
				Name:   "parse",
				Action: doParse,
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "from-html"},
					&cli.StringFlag{Name: "from-urls"},
					&cli.StringFlag{Name: "out", Value: "cards.json"},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.WithError(err).Fatal("failed to scrape")
	}
}

func doParse(c *cli.Context) error {
	fromURLs := c.IsSet("from-urls")
	fromHTML := c.IsSet("from-html")
	if (fromURLs && fromHTML) || (!fromURLs && !fromHTML) {
		return errors.Errorf("must provide exactly on of --from-urls or --from-html")
	}
	w := newParseWorker()
	if fromURLs {
		return w.FromURLs(c.String("from-urls"), c.String("out"))
	}
	if fromHTML {
		return nil
	}
	panic("unreachable")
}

type parseWorker struct {
	mu    *sync.Mutex
	Cards map[string]Card
}

func newParseWorker() *parseWorker {
	return &parseWorker{
		mu:    new(sync.Mutex),
		Cards: make(map[string]Card),
	}
}

func (w *parseWorker) FromURLs(urlsPath, outPath string) error {
	f, err := os.Open(urlsPath)
	if err != nil {
		return err
	}
	defer f.Close()

	urls := make(chan string)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			urls <- line
		}
		close(urls)
	}()

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.worker(urls)
		}()
	}

	wg.Wait()
	g, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer g.Close()
	return json.NewEncoder(g).Encode(w.Cards)
}

func (w *parseWorker) worker(urls <-chan string) {
	for u := range urls {
		start := time.Now()
		card, err := NewCardFromURL(u)
		if err != nil {
			log.WithError(err).Errorf("failed to parse new card: %s", u)
		}
		w.mu.Lock()
		w.Cards[u] = card
		w.mu.Unlock()
		log.WithField("elapsed", time.Since(start)).Infof("parsed card %s", u)
	}
}

// Constants for scraping such as URLs and CSS selectors.
const (
	BaseURL  = "https://yugioh.fandom.com/wiki/Category:TCG_cards"
	SelCards = ".category-page__member-link"
)

var baseURL *url.URL

func init() {
	var err error
	baseURL, err = url.Parse(BaseURL)
	if err != nil {
		log.WithError(err).Fatalf("failed to parse base url: %s", BaseURL)
	}
}

// AllCardURLs returns all scraped card urls, from the TCG card listing. Each
// page is read serially, until there are no more cards.
func AllCardURLs() ([]string, error) {
	var urls []string
	u := baseURL
	for {
		// TODO: Use actual "next page" links instead of using last
		// seen url as ?from= query param.
		more, next, err := linksOnePage(u)
		if err != nil {
			return urls, err
		}
		if len(more) == 0 {
			break
		}
		urls = append(urls, more...)
		u = next
		log.Infof("scraped %d card urls", len(urls))
	}
	return urls, nil
}

// TODO: proxy this
func fetchURL(u string) ([]byte, error) {
	resp, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

// Return all card urls from the given url, and return the next page to
// serially call this function on next.
func linksOnePage(u *url.URL) ([]string, *url.URL, error) {
	// If we have just begun scraping, then we want to start with the first
	// card link. Otherwise, the ?from= query param sets the first card
	// inclusively, which have already parsed in the previous page, so we
	// skip it.
	skip := 0
	if u.String() != baseURL.String() {
		skip = 1
	}
	resp, err := http.Get(u.String())
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, nil, errors.Errorf("bad status code: %v", resp.StatusCode)
	}
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	var urls []string
	var lastTitle string
	doc.Find(SelCards).Each(func(i int, s *goquery.Selection) {
		if i < skip {
			return
		}
		title, _ := s.Attr("title")
		href, _ := s.Attr("href")
		if title == "" || href == "" {
			log.Errorf("missing title or href attr: %s", s.Text())
			return
		}
		ref, err := url.Parse(href)
		if err != nil {
			log.Errorf("failed to parse href: %s", href)
			return
		}
		u := baseURL.ResolveReference(ref)
		urls = append(urls, u.String())
		lastTitle = title
	})
	ref, err := url.Parse(fmt.Sprintf("?from=%s", url.QueryEscape(lastTitle)))
	if err != nil {
		return nil, nil, errors.Errorf("failed to parse last page: %v", err)
	}
	next := baseURL.ResolveReference(ref)
	return urls, next, err
}

// WriteAllCardURLs scrapes all TCG card urls and writes each url as a separate
// line to a file at the given path. This function creates or overwrites a file
// at the given path.
func WriteAllCardURLs(path string) error {
	start := time.Now()
	urls, err := AllCardURLs()
	if err != nil {
		return errors.Wrap(err, "failed to scrape card urls")
	}
	f, err := os.Create(path)
	if err != nil {
		return errors.Wrapf(err, "failed to create file: %s", path)
	}
	defer f.Close()
	for _, u := range urls {
		if _, err := f.WriteString(fmt.Sprintf("%s\n", u)); err != nil {
			return err
		}
	}
	log.WithField("elapsed", time.Since(start)).Infof("finished writing %d urls to %s", len(urls), path)
	return nil
}

// Card represents a single card.
type Card struct {
	URL          string              `json:"url"`
	Image        []byte              `json:"image"`
	NameEnglish  string              `json:"name_en"`
	NameJapanese string              `json:"name_jp"`
	Type         string              `json:"type"`
	Attribute    string              `json:"attribute"`
	Types        []string            `json:"types"`
	Level        int                 `json:"level"`
	Attack       int                 `json:"attack"`
	Defense      int                 `json:"defense"`
	Passcode     int                 `json:"passcode"`
	EffectTypes  []string            `json:"effect_types"`
	Property     string              `json:"property"`
	Statuses     []string            `json:"statuses"`
	Description  string              `json:"description"`
	Releases     []Release           `json:"releases"`
	Categories   map[string][]string `json:"categories"`
}

// EmptyCard ...
var EmptyCard = Card{}

// Release ...
type Release struct {
	Date   time.Time `json:"date"`
	Number string    `json:"number"`
	Set    string    `json:"set"`
	Rarity string    `json:"rarity"`
}

var (
	reSplitSlash = regexp.MustCompile(`\s*/\s*`)
	reLevel      = regexp.MustCompile(`^\s*(\d+)`)
)

// NewCardFromURL parses a Card from the given url string.
func NewCardFromURL(u string) (Card, error) {
	resp, err := http.Get(u)
	if err != nil {
		return EmptyCard, err
	}
	defer resp.Body.Close()
	return NewCardFromReader(u, resp.Body)
}

// NewCardFromHTML is like NewCardFromURL, but it parses from an already
// fetched HTML file, instead of from the
func NewCardFromHTML(path string) (Card, error) {
	f, err := os.Open(path)
	if err != nil {
		return EmptyCard, err
	}
	defer f.Close()
	return NewCardFromReader(path, f)
}

// NewCardFromReader ...
func NewCardFromReader(src string, rdr io.Reader) (Card, error) {
	var card Card
	entry := log.WithField("src", src)

	doc, err := goquery.NewDocumentFromReader(rdr)
	if err != nil {
		return EmptyCard, err
	}

	// URL
	var ok bool
	card.URL, ok = doc.Find(`head meta[property="og:url"]`).First().Attr("content")
	if !ok {
		return EmptyCard, errors.Errorf("missing url")
	}

	// Image
	imgSrc, ok := doc.Find(`td[class="cardtable-cardimage"] img`).First().Attr("src")
	if !ok {
		return EmptyCard, errors.Errorf("missing img src")
	}
	// TODO: enable this
	if false {
		card.Image, err = fetchURL(imgSrc)
		if err != nil {
			return EmptyCard, err
		}
	}

	// Row fields, e.g. Name, Types, Level, etc.
	doc.Find(`table[class="cardtable"] tr[class="cardtablerow"]`).Each(
		func(i int, s *goquery.Selection) {
			header := clean(s.Find(`th[class="cardtablerowheader"]`).First().Text())
			data := clean(s.Find(`td[class="cardtablerowdata"]`).First().Text())
			switch header {
			case "English":
				card.NameEnglish = data
			case "Japanese", "Japanese (base)":
				card.NameJapanese = strings.Replace(data, "Check translation", "", -1)
			case "Card type":
				card.Type = data
			case "Property":
				card.Property = data
			case "Attribute":
				card.Attribute = data
			case "Types":
				card.Types = reSplitSlash.Split(data, -1)
			case "Level":
				groups := reLevel.FindStringSubmatch(data)
				if len(groups) != 2 {
					entry.Errorf("failed to parse level from '%s'", data)
					break
				}
				lvl, err := strconv.ParseInt(groups[1], 10, 0)
				if err != nil {
					entry.WithError(err).Errorf("failed to parse int: %s", groups[1])
					break
				}
				card.Level = int(lvl)
			case "ATK / DEF":
				parts := reSplitSlash.Split(data, -1)
				if len(parts) != 2 {
					entry.WithError(err).Errorf("ATK/DEF didn't have 2 parts: '%s'", data)
					break
				}
				atk, err := strconv.ParseInt(parts[0], 10, 0)
				if err != nil {
					entry.WithError(err).Errorf("failed to parse int: %s", parts[0])
					break
				}
				def, err := strconv.ParseInt(parts[1], 10, 0)
				if err != nil {
					entry.WithError(err).Errorf("failed to parse int: %s", parts[1])
					break
				}
				card.Attack = int(atk)
				card.Defense = int(def)
			case "Passcode":
				code, err := strconv.ParseInt(data, 10, 0)
				if err != nil {
					entry.WithError(err).Errorf("failed to parse int: %s", data)
					break
				}
				card.Passcode = int(code)
			case "Card effect types":
				types := s.Find(`a`).Map(func(i int, s *goquery.Selection) string {
					return s.Text()
				})
				card.EffectTypes = dedup(types)
			case "Statuses":
				card.Statuses = dedup(s.Find(`td[class="cardtablerowdata"] a`).
					Map(func(i int, s *goquery.Selection) string {
						return s.Text()
					}))
			case "", "French", "German", "Italian", "Korean", "Portuguese", "Spanish",
				"Japanese (kana)", "Japanese (rōmaji)",
				"Japanese (translated)", "External links":
				break // not relevant
			default:
				entry.Warnf("unknown header: '%s'", header)
			}
		})

	// Description
	card.Description = strings.TrimSpace(doc.Find(`.navbox-list`).First().Text())

	// Releases
	doc.Find(`td .navbox-list table[data-region="en"] tbody`).Each(func(i int, s *goquery.Selection) {
		s.Find(`tr`).Each(func(i int, s *goquery.Selection) {
			if i == 0 {
				// First item is header row. With the different
				// selector "th". We could validate this, and
				// for that matter the order of the fields, but
				// for now we punt on that complexity and
				// assume.
				return
			}
			// Release, Number, Set, Rarity
			parts := s.Find(`td`).Map(func(i int, s *goquery.Selection) string {
				return strings.TrimSpace(s.Text())
			})
			if len(parts) != 4 {
				entry.Errorf("releases row isn't len 4: %d", len(parts))
				return
			}
			t, err := time.Parse("2006-01-02", parts[0])
			if err != nil {
				entry.WithError(err).Errorf("failed to parse time: '%s'", parts[0])
				return
			}
			card.Releases = append(card.Releases, Release{
				Date:   t,
				Number: parts[1],
				Set:    parts[2],
				Rarity: parts[3],
			})
		})
	})

	// Search categories
	cats := make(map[string][]string)
	doc.Find(`div.cardtable-categories div.hlist`).Each(func(i int, s *goquery.Selection) {
		cat := clean(s.Find(`dt`).First().Text())
		vals := dedup(s.Find(`dd`).Map(func(i int, s *goquery.Selection) string {
			return s.Text()
		}))
		cats[cat] = vals
	})
	card.Categories = cats

	return card, nil
}

func dedup(slice []string) []string {
	seen := make(map[string]bool)
	var deduped []string
	for _, s := range slice {
		s = clean(s)
		if seen[s] {
			continue
		}
		seen[s] = true
		deduped = append(deduped, s)
	}
	sort.Strings(deduped)
	return deduped
}

func clean(s string) string {
	return strings.TrimSpace(s)
}
