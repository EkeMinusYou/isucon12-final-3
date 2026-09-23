package main

import (
	"bytes"
	"testing"
)

func TestISUCON12FinalResultMarkers(t *testing.T) {
	declared, err := loadPatterns("../contest/bench-patterns.json")
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	w := &resultWriter{out: &output, patterns: declared}
	input := `[ADMIN] 14:51:37.431866 [SCORE] map[POST /login:6 POST /login(ban):119]
14:51:37.431850 [PASSED]: true
14:51:37.431852 [SCORE] 0 (addition: 260, deduction: 300)
`
	if _, err := w.Write([]byte(input)); err != nil {
		t.Fatal(err)
	}
	if got := string(w.markers()); got != "SCORE: 0\nBENCHMARK_PASS\n" {
		t.Fatalf("markers = %q", got)
	}
}
