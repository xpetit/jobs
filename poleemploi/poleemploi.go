package poleemploi

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/xpetit/x/v2"
)

const (
	MaxItemsPerPage = 150
	MaxPages        = 21
	MaxItems        = MaxItemsPerPage * MaxPages

	MaxRequestsPerSecond = 4
)

type poleEmploi struct {
	m              sync.Mutex
	authExpiration time.Time
	nextTime       time.Time
	token          string
	id             string
	secret         string
}

// auth renew token if necessary and ensures rate limiting is applied
func (p *poleEmploi) auth() {
	p.m.Lock()
	defer p.m.Unlock()

	// rate limit
	now := time.Now()
	time.Sleep(p.nextTime.Sub(now))
	p.nextTime = now.Add(time.Second / MaxRequestsPerSecond)

	if time.Now().Before(p.authExpiration) {
		return
	}

	// renew token
	resp := C2(http.Post(
		"https://entreprise.pole-emploi.fr/connexion/oauth2/access_token?"+url.PathEscape("realm=/partenaire"),
		"application/x-www-form-urlencoded",
		strings.NewReader(url.Values{
			"grant_type":    {"client_credentials"},
			"scope":         {"api_offresdemploiv2 o2dsoffre"},
			"client_id":     {p.id},
			"client_secret": {p.secret},
		}.Encode()),
	))
	var cred struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	C(json.NewDecoder(CloseAfterRead(resp.Body)).Decode(&cred))
	validityDuration := time.Duration(cred.ExpiresIn) * time.Second
	p.authExpiration = time.Now().Add(validityDuration - 10*time.Second)
	p.token = "Bearer " + cred.AccessToken
}

func NewAPI(id, secret string) *poleEmploi {
	p := &poleEmploi{id: id, secret: secret}
	p.auth()
	return p
}

func (p *poleEmploi) Get(path string) (b []byte, remaining int) {
	p.auth()

	req := C2(http.NewRequest("GET", "https://api.pole-emploi.io/partenaire/offresdemploi/v2/offres/"+path, nil))
	req.Header.Set("Authorization", p.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil { // retry
		log.Println(err)
		return p.Get(path)
	}
	defer Closing(resp.Body)

	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil, 0

	case http.StatusTooManyRequests: // retry
		log.Println(http.StatusText(http.StatusTooManyRequests))
		return p.Get(path)

	case http.StatusOK, http.StatusPartialContent:
		if resp.StatusCode == http.StatusPartialContent {
			_, s, _ := strings.Cut(resp.Header.Get("Content-Range"), "/")
			remaining = C2(strconv.Atoi(s))
		}
		b, err := io.ReadAll(resp.Body)
		if err != nil { // retry
			log.Println(err)
			return p.Get(path)
		}
		return b, remaining
	}
	panic(fmt.Sprint("requesting pole emploi: ", http.StatusText(resp.StatusCode)))
}

func (p *poleEmploi) Search(minDate, maxDate time.Time, page, nbItems int) (b []byte, remaining int) {
	Assert(page >= 0 && page < MaxPages)
	Assert(nbItems > 0 && nbItems <= MaxItemsPerPage)
	return p.Get("search?" + url.Values{
		"range":           {fmt.Sprintf("%d-%d", MaxItemsPerPage*page, MaxItemsPerPage*page+nbItems-1)},
		"sort":            {"1"},
		"minCreationDate": {minDate.Format("2006-01-02T15:04:05Z")},
		"maxCreationDate": {maxDate.Format("2006-01-02T15:04:05Z")},
	}.Encode())
}
