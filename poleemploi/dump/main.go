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

func formattedTimeToSeconds(s string) uint32 {
	return uint32(C2(time.ParseInLocation(time.DateTime, s, time.Local)).Unix())
}

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
	wait := Goroutines(8, func() {
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

					{ // clean "description" field
						var s string
						C(json.Unmarshal(data["description"], &s))
						if strings.Contains(s, `Ã©`) { // fix encoding issue
							s = C2(latin1.String(s))
						}
						s, _ = C3(transform.String(t, s))
						s = html.UnescapeString(s)
						data["description"] = C2(json.Marshal(s))
					}

					result = C2(json.Marshal(data))
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
	wait()
	log.Printf("saved %d job offers at a rate of %.1f req/sec (maximum allowed: %d)\n",
		saved.Load(),
		float64(reqCount.Load())/time.Since(t).Seconds(),
		poleemploi.MaxRequestsPerSecond,
	)
}
