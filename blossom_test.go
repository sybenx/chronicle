package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"fiatjaf.com/nostr"
)

func TestIsPublicAddr(t *testing.T) {
	cases := map[string]bool{
		"1.1.1.1":         true,
		"2606:4700::1111": true,
		"127.0.0.1":       false,
		"10.0.0.5":        false,
		"192.168.1.20":    false,
		"172.16.0.1":      false,
		"100.101.102.103": false,
		"169.254.169.254": false,
		"0.0.0.0":         false,
		"::1":             false,
		"fd7a:115c::1":    false,
		"fe80::1":         false,
		"::ffff:10.0.0.1": false,
		"64:ff9b::a00:1":  false,
	}
	for ip, want := range cases {
		if got := isPublicAddr(netip.MustParseAddr(ip)); got != want {
			t.Errorf("isPublicAddr(%s) = %v, want %v", ip, got, want)
		}
	}
}

func TestDownloadRefusesLoopback(t *testing.T) {
	config.BlossomAssetsPath = t.TempDir() + "/"
	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
	}))
	defer srv.Close()

	hash := strings.Repeat("a", 64)
	err := downloadBlossomFile(srv.URL+"/"+hash, hash, noSizeLimit)
	if !errors.Is(err, errNonPublicAddress) {
		t.Fatalf("expected non-public refusal, got %v", err)
	}
	if hit {
		t.Fatal("request reached the loopback server")
	}

	if err := downloadBlossomFile("file:///etc/passwd", hash, noSizeLimit); err == nil {
		t.Fatal("expected file:// to be refused")
	}
}

func TestExtractMediaRefs(t *testing.T) {
	h1 := strings.Repeat("1", 64)
	h2 := strings.Repeat("2", 64)
	h3 := strings.Repeat("3", 64)
	event := nostr.Event{
		Content: "look https://a.example/" + h1 + ".jpg and https://cdn.example/pretty-name.mp4",
		Tags: nostr.Tags{
			{"imeta", "url https://cdn.example/pretty-name.mp4", "x " + h2, "fallback https://b.example/" + h2 + ".mp4"},
			{"imeta", "url https://c.example/" + h3, "m image/png"},
			{"imeta", "url https://a.example/" + h1 + ".jpg", "x " + h1},
			{"imeta", "url https://d.example/no-hash.png"},
		},
	}

	refs := extractMediaRefs(event)
	if len(refs) != 3 {
		t.Fatalf("got %d refs, want 3: %+v", len(refs), refs)
	}
	if refs[0].hash != h1 || len(refs[0].urls) != 1 {
		t.Errorf("content ref not deduplicated with its imeta tag: %+v", refs[0])
	}
	if refs[1].hash != h2 || len(refs[1].urls) != 2 || refs[1].urls[0] != "https://cdn.example/pretty-name.mp4" {
		t.Errorf("imeta x ref wrong: %+v", refs[1])
	}
	if refs[2].hash != h3 {
		t.Errorf("hash not taken from imeta url: %+v", refs[2])
	}
}

func TestMediaSizeLimit(t *testing.T) {
	owner := nostr.Generate()
	other := nostr.Generate()
	config.OwnerPubkey = owner.Public().Hex()
	config.MaxFileSizeMB = 10
	config.ReactedMaxFileSizeMB = 100

	ownEvent := nostr.Event{PubKey: owner.Public()}
	plain := nostr.Event{ID: nostr.ID{1}, PubKey: other.Public()}
	reacted := nostr.Event{ID: nostr.ID{2}, PubKey: other.Public()}
	markReactedTarget(reacted.ID)

	if got := mediaSizeLimit(ownEvent); got != noSizeLimit {
		t.Errorf("owner limit = %d, want none", got)
	}
	if got := mediaSizeLimit(plain); got != 10<<20 {
		t.Errorf("plain limit = %d", got)
	}
	if got := mediaSizeLimit(reacted); got != 100<<20 {
		t.Errorf("reacted limit = %d", got)
	}
}
