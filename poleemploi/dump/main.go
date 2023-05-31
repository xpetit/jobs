package main

import (
	"encoding/json"
	"flag"
	"html"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/klauspost/compress/zstd"
	"github.com/xpetit/jobs/poleemploi"
	. "github.com/xpetit/x/v2"
	"golang.org/x/term"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// span of time with second resolution
type span struct{ min, max uint32 }

// split splits a span into two halves
func (sp span) split() (span, span) {
	mid := sp.min + (sp.max-sp.min)/2
	return span{
			min: sp.min,
			max: mid - 1,
		}, span{
			min: mid,
			max: sp.max,
		}
}

// formattedTimeToSeconds converts a local datetime "2019-01-01 00:00:00" to Unix time
func formattedTimeToSeconds(s string) uint32 {
	return uint32(C2(time.ParseInLocation(time.DateTime, s, time.Local)).Unix())
}

// cleanLine removes unecessary tokens from the non-empty line
func cleanLine(line string) string {
	fields := strings.Fields(line)
	keepFields := make([]string, 0, len(fields))
	for i, field := range fields {
		// let's ignore fields like "F/H", "hf", "((F\H))", "*F/H*" and so on...
		lettersAndNumbers := strings.Map(func(r rune) rune {
			if unicode.In(r, unicode.Letter, unicode.Number) {
				return r
			}
			return -1
		}, field)
		switch strings.ToLower(lettersAndNumbers) {
		case "hf", "fh":
			continue
		}

		field = strings.ReplaceAll(field, "¿", "")
		if field == "" {
			continue
		}

		// ignore duplicated field that don't contain spaces, numbers or letters
		// this can happen with the following line : "abc - (F/H) - abc", where the hyphen is duplicated
		if i > 0 && len(keepFields) > 0 && lettersAndNumbers == "" && field == keepFields[len(keepFields)-1] {
			continue
		}

		keepFields = append(keepFields, field)
	}
	return strings.Join(keepFields, " ")
}

// cleanText reformats jobs-related text, it removes consecutive empty lines and unecessary tokens
// TODO: handle '\'
// TODO: handle residual "h/f"
func cleanText(text string) string {
	text = strings.ReplaceAll(text, "/r/n", "\n")

	var prevLineIsEmpty bool
	lines := strings.Split(strings.TrimSpace(text), "\n")
	keepLines := make([]string, 0, len(lines))
	for _, line := range lines {
		if line = strings.TrimSpace(line); line == "" {
			if prevLineIsEmpty {
				// we already kept an empty line, let's not add more
				continue
			}
			// we keep a single empty line
			prevLineIsEmpty = true
			keepLines = append(keepLines, "")
		} else { // line != ""
			prevLineIsEmpty = false
			keepLines = append(keepLines, cleanLine(line))
		}
	}
	return strings.Join(keepLines, "\n")
}

func main() {
	log.SetFlags(0)

	now := time.Now()
	var (
		id      = flag.String("id", "", "Pole Emploi API ID (https://pole-emploi.io/data/api/offres-emploi)")
		secret  = flag.String("secret", "", "Pole Emploi API secret key")
		minDate = flag.String("min", now.AddDate(-1, 0, 0).Format(time.DateTime), "Minimum date in local time")
		maxDate = flag.String("max", now.Format(time.DateTime), "Maximum date in local time")
	)
	flag.Parse()

	Assert(!term.IsTerminal(int(os.Stdout.Fd())), "The output is not a terminal")

	api := poleemploi.NewAPI(*id, *secret)
	var reqCount atomic.Uint32
	search := func(sp span, page, nbItems int) ([]byte, int) {
		reqCount.Add(1)
		return api.Search(
			time.Unix(int64(sp.min), 0),
			time.Unix(int64(sp.max), 0),
			page,
			nbItems,
		)
	}

	var outMu sync.Mutex
	out := C2(zstd.NewWriter(os.Stdout))
	defer Closing(out)

	var idsMu sync.Mutex
	ids := map[string]struct{}{}

	var saved atomic.Uint32
	type job struct {
		sp        span
		remaining int
	}
	jobs := make(chan job, 16)
	waitForWorkers := Goroutines(8, func() {
		latin1 := charmap.ISO8859_1.NewEncoder()
		t := transform.Chain(norm.NFKC) // https://fr.wikipedia.org/wiki/Normalisation_Unicode

		for job := range jobs {
			for page := 0; job.remaining > 0; page++ {
				b, remaining := search(job.sp, page, Min(job.remaining, poleemploi.MaxItemsPerPage))
				if len(b) == 0 {
					Assert(remaining == 0)
					break
				}
				var data struct {
					Results []json.RawMessage `json:"resultats"`
				}
				C(json.Unmarshal(b, &data))
				Assert(len(data.Results) > 0 && len(data.Results) <= poleemploi.MaxItemsPerPage)

				for _, result := range data.Results {
					var data map[string]json.RawMessage
					C(json.Unmarshal(result, &data))

					// deduplicate on ID
					id := string(data["id"])
					idsMu.Lock()
					_, duplicate := ids[id]
					if !duplicate {
						ids[id] = struct{}{}
					}
					idsMu.Unlock()
					if duplicate {
						continue
					}

					{ // clean "title" field
						var s string
						C(json.Unmarshal(data["intitule"], &s))
						s = cleanText(s)
						data["intitule"] = C2(json.Marshal(s))
					}
					{ // clean "description" field
						var s string
						C(json.Unmarshal(data["description"], &s))
						if strings.Contains(s, `Ã©`) { // fix encoding issue
							s = C2(latin1.String(s))
						}
						s, _ = C3(transform.String(t, s))
						s = html.UnescapeString(s)
						s = cleanText(s)
						data["description"] = C2(json.Marshal(s))
					}

					result = append(C2(json.Marshal(data)), '\n')
					outMu.Lock()
					C2(out.Write(result))
					outMu.Unlock()
					saved.Add(1)
				}
				job.remaining -= poleemploi.MaxItemsPerPage
			}
		}
	})

	entireSpan := span{formattedTimeToSeconds(*minDate), formattedTimeToSeconds(*maxDate)}
	_, remaining := search(entireSpan, 0, 1)
	log.Println("saving", remaining, "job offers")

	t := time.Now()
	spans := []span{entireSpan}
	for len(spans) > 0 {
		// pop last span
		sp := spans[len(spans)-1]
		spans = spans[:len(spans)-1]

		_, remaining := search(sp, 0, 1)
		if remaining >= poleemploi.MaxItems {
			// time span is too large for pole emploi API pagination, so split in half
			a, b := sp.split()
			spans = append(spans, b, a)

		} else if remaining > 0 {
			jobs <- job{sp, remaining}
		}
	}
	close(jobs)
	waitForWorkers()
	log.Printf("saved %d job offers at a rate of %.1f req/sec (maximum allowed: %d)\n",
		saved.Load(),
		float64(reqCount.Load())/time.Since(t).Seconds(),
		poleemploi.MaxRequestsPerSecond,
	)
}
