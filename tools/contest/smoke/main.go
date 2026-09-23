// Command smoke makes one registration and two authenticated reads without
// running the benchmark. It never prints session identifiers or credentials.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

func main() {
	base := flag.String("base", "http://127.0.0.1", "application base URL, reached from the host this runs on")
	flag.Parse()

	c := &client{http: &http.Client{Timeout: 15 * time.Second}, base: strings.TrimSuffix(*base, "/")}
	requestBody, _ := json.Marshal(map[string]interface{}{"viewerId": fmt.Sprintf("setup-%d", time.Now().UnixNano()), "platformType": 1})
	response, err := c.do(http.MethodPost, "/user", bytes.NewReader(requestBody))
	if err != nil {
		log.Fatal(err)
	}
	var created struct {
		UserID    int64  `json:"userId"`
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(response, &created); err != nil || created.UserID == 0 || created.SessionID == "" {
		log.Fatal("registration did not return a user and session")
	}
	c.session = created.SessionID
	for _, path := range []string{fmt.Sprintf("/user/%d/home", created.UserID), fmt.Sprintf("/user/%d/item", created.UserID)} {
		if _, err := c.do(http.MethodGet, path, nil); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("Smoke: registration and two authenticated reads passed")
}

type client struct {
	http    *http.Client
	base    string
	session string
}

func (c *client) do(method, path string, body io.Reader) ([]byte, error) {
	req, err := http.NewRequest(method, c.base+path, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("x-isu-date", "Sat, 06 Aug 2022 12:00:00 GMT")
	req.Header.Set("x-master-version", "1")
	if c.session != "" {
		req.Header.Set("x-session", c.session)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	if res.StatusCode != http.StatusOK {
		// Response bodies can carry identifiers, so report the status only.
		return nil, fmt.Errorf("%s %s: status %d", method, path, res.StatusCode)
	}
	return data, nil
}
