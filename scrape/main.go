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
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/hashicorp/go-retryablehttp"
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
					&cli.StringFlag{
						Name:  "path",
						Value: "card-urls.txt",
						Usage: "Path to write card URLs to, on each line.",
					},
				},
			},
			{
				Name:   "parse",
				Action: doParse,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "from-html",
						Usage: "Path to HTML pre-fetched card or directory of.",
					},
					&cli.StringFlag{
						Name:  "from-urls",
						Usage: "Path to text file of card URLs, one on each line.",
					},
					&cli.StringFlag{
						Name:  "cards",
						Value: "cards.json",
						Usage: "Where to write JSON cards to.",
					},
					&cli.StringFlag{
						Name:  "images",
						Usage: "If set, path to JSON with fetched image bytes.",
					},
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
		return errors.Errorf("must provide exactly one of --from-urls or --from-html")
	}
	w := newParseWorker(c.String("images"))
	if fromURLs {
		return w.FromURLs(c.String("from-urls"), c.String("cards"))
	}
	if fromHTML {
		return w.FromHTML(c.String("from-html"), c.String("cards"))
	}
	panic("unreachable")
}

type parseWorker struct {
	outImages string

	mu     *sync.Mutex
	cards  []SrcCard
	images []SrcImage
}

// SrcCard ...
type SrcCard struct {
	Src  string `json:"src"`
	Card Card   `json:"card"`
}

// SrcImage ...
type SrcImage struct {
	Src   string `json:"src"`
	Image []byte `json:"image"`
}

func newParseWorker(outImages string) *parseWorker {
	return &parseWorker{
		outImages: outImages,
		mu:        new(sync.Mutex),
	}
}

func (w *parseWorker) FromURLs(urlsPath, outCards string) error {
	f, err := os.Open(urlsPath)
	if err != nil {
		return err
	}
	defer f.Close()

	requests := make(chan workRequest)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			requests <- newRequestURL(line)
		}
		close(requests)
	}()
	return w.FinishWork(requests, outCards)
}

func (w *parseWorker) FromHTML(file, out string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	requests := make(chan workRequest)
	go func() {
		if err := w.forEachHTML(file, func(path string) {
			requests <- newRequestHTML(path)
		}); err != nil {
			log.WithError(err).Error("failed to traverse and parse")
		}
		close(requests)
	}()
	return w.FinishWork(requests, out)
}

func (w *parseWorker) FinishWork(
	requests <-chan workRequest,
	outCards string,
) error {
	wg := new(sync.WaitGroup)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.worker(requests)
		}()
	}
	wg.Wait()
	if err := w.save(outCards, w.cards); err != nil {
		return err
	}
	if w.outImages != "" {
		if err := w.save(w.outImages, w.images); err != nil {
			return err
		}

	}
	return nil
}

func (w *parseWorker) save(out string, thing interface{}) error {
	f, err := os.Create(out)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", strings.Repeat(" ", 4))
	return enc.Encode(thing)
}

// ugh, autogenerate magic i summon you, do thy bidding and abstract!

type workRequest struct {
	url  *string
	file *string
}

func newRequestURL(u string) workRequest {
	return workRequest{url: &u}
}

func newRequestHTML(path string) workRequest {
	return workRequest{file: &path}
}

func (r workRequest) PeekURL() (string, bool) {
	if r.url != nil && r.file != nil {
		panic("invalid work request, all non-nil")
	}
	if r.url != nil {
		return *r.url, true
	}
	return "", false
}

func (r workRequest) PeekFile() (string, bool) {
	if r.url != nil && r.file != nil {
		panic("invalid work request, all non-nil")
	}
	if r.file != nil {
		return *r.file, true
	}
	return "", false
}

func (w *parseWorker) worker(requests <-chan workRequest) {
	for r := range requests {
		start := time.Now()
		var src string
		if u, ok := r.PeekURL(); ok {
			src = u
			card, err := NewCardFromURL(u, w.outImages != "")
			if err != nil {
				log.WithError(err).Errorf("failed to parse new card: %s", u)
				return
			}
			w.addCard(u, card)
			if w.outImages != "" {
				w.addImage(u, card.Image)
			}
		} else if file, ok := r.PeekFile(); ok {
			src = file
			card, err := NewCardFromHTML(file, w.outImages != "")
			if err != nil {
				log.WithError(err).Errorf("failed to parse new card: %s", file)
				return
			}
			w.addCard(file, card)
			if w.outImages != "" {
				w.addImage(file, card.Image)
			}
		} else {
			panic("invalid work request, neither url nor html or dir")
		}
		log.WithField("elapsed", time.Since(start)).Infof("parsed card %s", src)
	}
}

func (w *parseWorker) forEachHTML(root string, f func(path string)) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		f(path)
		return nil
	})
}

func (w *parseWorker) addCard(src string, card Card) {
	w.mu.Lock()
	w.cards = append(w.cards, SrcCard{src, card})
	w.mu.Unlock()
}

func (w *parseWorker) addImage(src string, img []byte) {
	w.mu.Lock()
	w.images = append(w.images, SrcImage{src, img})
	w.mu.Unlock()
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
	token := os.Getenv("PROXYCRAWL_TOKEN")
	if token == "" {
		log.Warning("not using token, env POXYCRAWL_TOKEN not set")
		resp, err := retryablehttp.Get(u)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return ioutil.ReadAll(resp.Body)
	}
	escaped := url.QueryEscape(u)
	resp, err := http.Get(fmt.Sprintf("https://api.proxycrawl.com/?token=%s&url=%s", token, escaped))
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
	Image           []byte              `json:"-"`
	URL             string              `json:"url"`
	NameEnglish     string              `json:"name_en"`
	NameJapanese    string              `json:"name_jp"`
	OtherNames      string              `json:"other_names"`
	Type            string              `json:"type"`
	Attribute       string              `json:"attribute"`
	Types           []string            `json:"types"`
	Level           int                 `json:"level"`
	Rank            int                 `json:"rank"`
	Materials       string              `json:"materials"`
	FusionMaterial  []string            `json:"fusion_material"`
	RitualSpell     string              `json:"ritual_spell"`
	RitualMonster   string              `json:"ritual_monster"`
	SummonedBy      []string            `json:"summoned_by"`
	SynchroMaterial []string            `json:"synchro_material"`
	PendulumScale   int                 `json:"pendulum_scale"`
	LimitationText  string              `json:"limitation_text"`
	Attack          string              `json:"attack"`
	Defense         string              `json:"defense"`
	Passcode        int                 `json:"passcode"`
	EffectTypes     []string            `json:"effect_types"`
	Property        string              `json:"property"`
	Statuses        []string            `json:"statuses"`
	Description     string              `json:"description"`
	Releases        []Release           `json:"releases"`
	Categories      map[string][]string `json:"categories"`
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
	reSplit       = regexp.MustCompile(`\s*(/|,)\s*`)
	reFirstNumber = regexp.MustCompile(`^\s*(\d+)`)
	reLastNumber  = regexp.MustCompile(`(\d+)\s*$`)
)

// NewCardFromURL parses a Card from the given url string.
func NewCardFromURL(u string, fetchImage bool) (Card, error) {
	resp, err := http.Get(u)
	if err != nil {
		return EmptyCard, err
	}
	defer resp.Body.Close()
	return NewCardFromReader(u, resp.Body, fetchImage)
}

// NewCardFromHTML is like NewCardFromURL, but it parses from an already
// fetched HTML file, instead of from the
func NewCardFromHTML(path string, fetchImage bool) (Card, error) {
	f, err := os.Open(path)
	if err != nil {
		return EmptyCard, err
	}
	defer f.Close()
	return NewCardFromReader(path, f, fetchImage)
}

// NewCardFromReader ...
func NewCardFromReader(
	src string,
	rdr io.Reader,
	fetchImage bool,
) (Card, error) {
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
	entry = entry.WithField("url", card.URL)

	// Image
	if fetchImage {
		imgSrc, ok := doc.Find(`td[class="cardtable-cardimage"] img`).First().Attr("src")
		if !ok {
			log.WithError(err).Errorf("missing img src")
		} else {
			card.Image, err = fetchURL(imgSrc)
			if err != nil {
				log.WithError(err).Errorf("failed to fetch image: %s", imgSrc)
			}
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
			case "Other names":
				card.OtherNames = data
			case "Card type":
				card.Type = data
			case "Property":
				card.Property = data
			case "Attribute":
				card.Attribute = data
			case "Types", "Type":
				card.Types = reSplit.Split(data, -1)
			case "Level":
				groups := reFirstNumber.FindStringSubmatch(data)
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
			case "Rank":
				groups := reFirstNumber.FindStringSubmatch(data)
				if len(groups) != 2 {
					entry.Errorf("failed to parse level from '%s'", data)
					break
				}
				rank, err := strconv.ParseInt(groups[1], 10, 0)
				if err != nil {
					entry.WithError(err).Errorf("failed to parse int: %s", groups[1])
					break
				}
				card.Rank = int(rank)
			case "Materials":
				card.Materials = data
			case "Fusion Material":
				types := s.Find(`a`).Map(func(i int, s *goquery.Selection) string {
					return s.Text()
				})
				card.FusionMaterial = dedup(types)
			case "Ritual Spell Card required":
				card.RitualSpell = data
			case "Ritual Monster required":
				card.RitualMonster = data
			case "Summoned by the effect of":
				types := s.Find(`a`).Map(func(i int, s *goquery.Selection) string {
					return s.Text()
				})
				card.SummonedBy = dedup(types)
			case "Synchro Material":
				types := s.Find(`a`).Map(func(i int, s *goquery.Selection) string {
					return s.Text()
				})
				card.SynchroMaterial = dedup(types)
			case "Pendulum Scale":
				groups := reLastNumber.FindStringSubmatch(data)
				if len(groups) != 2 {
					entry.Errorf("failed to parse pendulum scale from '%s'", data)
					break
				}
				scale, err := strconv.ParseInt(groups[1], 10, 0)
				if err != nil {
					entry.WithError(err).Errorf("failed to parse int: %s", data)
					break
				}
				card.PendulumScale = int(scale)
			case "Limitation text":
				card.LimitationText = data
			case "ATK / DEF":
				parts := reSplit.Split(data, -1)
				if len(parts) != 2 {
					entry.WithError(err).Errorf("ATK/DEF didn't have 2 parts: '%s'", data)
					break
				}
				card.Attack = clean(parts[0])
				card.Defense = clean(parts[1])
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
				"Japanese (translated)", "Other names (Japanese)",
				"Chinese", "External links":
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
			var t time.Time
			if parts[0] != "" {
				t, err = time.Parse("2006-01-02", parts[0])
				if err != nil {
					entry.WithError(err).Errorf("failed to parse time: '%s'", parts[0])
					return
				}
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
