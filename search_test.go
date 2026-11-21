package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"fiatjaf.com/nostr"
	"fiatjaf.com/nostr/eventstore/slicestore"
	"fiatjaf.com/nostr/khatru"
	"fiatjaf.com/nostr/khatru/policies"
)

func signedEvent(t *testing.T, sk nostr.SecretKey, kind nostr.Kind, content string, createdAt nostr.Timestamp) nostr.Event {
	t.Helper()
	evt := nostr.Event{PubKey: sk.Public(), CreatedAt: createdAt, Kind: kind, Content: content, Tags: nostr.Tags{}}
	if kind.IsAddressable() {
		evt.Tags = nostr.Tags{{"d", "article"}}
	}
	if err := evt.Sign(sk); err != nil {
		t.Fatal(err)
	}
	return evt
}

type searchFixture struct {
	url      string
	store    *searchStore
	ownerSK  nostr.SecretKey
	note     nostr.Event
	article  nostr.Event
	giftWrap nostr.Event
	manent   nostr.Event
}

// newSearchRelay wires a relay the way main does: search store, owner-only
// search policy and the restricted-kinds protection
func newSearchRelay(t *testing.T) *searchFixture {
	t.Helper()
	ownerSK := nostr.Generate()
	ownerPK := ownerSK.Public()

	raw := &slicestore.SliceStore{}
	raw.Init()
	s, err := newSearchStore(raw, t.TempDir()+"/search")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.index.Close)

	relay := khatru.NewRelay()
	relay.UseEventstore(s, 500)
	protectRestrictedEvents(relay, ownerPK)
	relay.OnRequest = policies.SeqRequest(searchOwnerOnly(ownerPK), noEmptyFiltersExceptSearch)

	server := httptest.NewServer(relay)
	t.Cleanup(server.Close)

	now := nostr.Now()
	f := &searchFixture{
		url:      "ws" + server.URL[4:],
		store:    s,
		ownerSK:  ownerSK,
		note:     signedEvent(t, ownerSK, nostr.KindTextNote, "thoughts about sourdough bread", now-30),
		article:  signedEvent(t, ownerSK, nostr.KindArticle, "a long essay on sourdough starters", now-20),
		giftWrap: signedEvent(t, ownerSK, nostr.KindGiftWrap, "sourdough secret", now-10),
		manent:   signedEvent(t, ownerSK, 33301, "sourdough private note", now-5),
	}
	for _, evt := range []nostr.Event{f.note, f.giftWrap, f.manent} {
		if err := s.SaveEvent(evt); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ReplaceEvent(f.article); err != nil {
		t.Fatal(err)
	}
	return f
}

// runREQ runs one REQ and returns the events, or the CLOSED reason
func runREQ(t *testing.T, r *nostr.Relay, filter nostr.Filter) ([]nostr.Event, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	sub, err := r.Subscribe(ctx, filter, nostr.SubscriptionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsub()
	var events []nostr.Event
	for {
		select {
		case evt := <-sub.Events:
			events = append(events, evt)
		case <-sub.EndOfStoredEvents:
			return events, ""
		case reason := <-sub.ClosedReason:
			return events, reason
		case <-ctx.Done():
			t.Fatal("query timed out")
		}
	}
}

func connect(t *testing.T, url string, sk *nostr.SecretKey) *nostr.Relay {
	t.Helper()
	r, err := nostr.RelayConnect(t.Context(), url, nostr.RelayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	if sk != nil {
		// khatru sends the AUTH challenge with the first auth-required refusal
		runREQ(t, r, nostr.Filter{Search: "challenge"})
		var authErr error
		for range 50 {
			if authErr = r.Auth(t.Context(), func(ctx context.Context, evt *nostr.Event) error {
				return evt.Sign(*sk)
			}); authErr == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if authErr != nil {
			t.Fatalf("auth failed: %v", authErr)
		}
	}
	return r
}

func ids(events []nostr.Event) []nostr.ID {
	out := make([]nostr.ID, len(events))
	for i, e := range events {
		out[i] = e.ID
	}
	return out
}

func TestSearchOwnerGetsMatches(t *testing.T) {
	f := newSearchRelay(t)
	r := connect(t, f.url, &f.ownerSK)

	events, closed := runREQ(t, r, nostr.Filter{Search: "sourdough"})
	if closed != "" {
		t.Fatalf("owner search refused: %s", closed)
	}
	got := ids(events)
	if len(got) != 2 || !slices.Contains(got, f.note.ID) || !slices.Contains(got, f.article.ID) {
		t.Fatalf("expected note and article, got %v", got)
	}

	events, _ = runREQ(t, r, nostr.Filter{Search: "sourdough", Kinds: []nostr.Kind{nostr.KindArticle}})
	if len(events) != 1 || events[0].ID != f.article.ID {
		t.Fatalf("kind filter not applied: %v", ids(events))
	}

	events, _ = runREQ(t, r, nostr.Filter{Search: "pizza"})
	if len(events) != 0 {
		t.Fatalf("unexpected matches: %v", ids(events))
	}
}

func TestSearchRestrictedKindsNeverReturned(t *testing.T) {
	f := newSearchRelay(t)
	r := connect(t, f.url, &f.ownerSK)

	// Even the owner asking for those kinds by name gets nothing from search
	events, closed := runREQ(t, r, nostr.Filter{Search: "sourdough", Kinds: []nostr.Kind{nostr.KindGiftWrap, 33301, 33302}})
	if closed != "" {
		t.Fatalf("refused: %s", closed)
	}
	if len(events) != 0 {
		t.Fatalf("restricted events came back from search: %v", ids(events))
	}
	for _, kind := range restrictedKinds {
		if isSearchableKind(kind) {
			t.Fatalf("restricted kind %d is searchable", kind)
		}
	}
	count, err := f.store.index.DocCount()
	if err != nil || count != 2 {
		t.Fatalf("index should hold only note and article, has %d (%v)", count, err)
	}
}

func TestSearchRefusedToOthers(t *testing.T) {
	f := newSearchRelay(t)

	anon := connect(t, f.url, nil)
	events, closed := runREQ(t, anon, nostr.Filter{Search: "sourdough"})
	if len(events) != 0 || len(closed) < 14 || closed[:14] != "auth-required:" {
		t.Fatalf("anonymous search not refused: %q %v", closed, ids(events))
	}

	stranger := nostr.Generate()
	other := connect(t, f.url, &stranger)
	events, closed = runREQ(t, other, nostr.Filter{Search: "sourdough"})
	if len(events) != 0 || len(closed) < 11 || closed[:11] != "restricted:" {
		t.Fatalf("other key's search not refused: %q %v", closed, ids(events))
	}

	// Plain reads stay public
	events, closed = runREQ(t, anon, nostr.Filter{Kinds: []nostr.Kind{nostr.KindTextNote}})
	if closed != "" || len(events) != 1 || events[0].ID != f.note.ID {
		t.Fatalf("public read broken: %q %v", closed, ids(events))
	}
}

func TestSearchReplaceAndDelete(t *testing.T) {
	f := newSearchRelay(t)
	newer := signedEvent(t, f.ownerSK, nostr.KindArticle, "rewritten about rye", f.article.CreatedAt+1)
	if err := f.store.ReplaceEvent(newer); err != nil {
		t.Fatal(err)
	}
	// An older version arriving late must not be indexed
	older := signedEvent(t, f.ownerSK, nostr.KindArticle, "ancient sourdough draft", f.article.CreatedAt-100)
	if err := f.store.ReplaceEvent(older); err != nil {
		t.Fatal(err)
	}
	if err := f.store.DeleteEvent(f.note.ID); err != nil {
		t.Fatal(err)
	}

	got := slices.Collect(f.store.QueryEvents(nostr.Filter{Search: "sourdough"}, 10))
	if len(got) != 0 {
		t.Fatalf("stale results: %v", ids(got))
	}
	got = slices.Collect(f.store.QueryEvents(nostr.Filter{Search: "rye"}, 10))
	if len(got) != 1 || got[0].ID != newer.ID {
		t.Fatalf("replacement not indexed: %v", ids(got))
	}
}

func TestMigrationIndexesExistingEvents(t *testing.T) {
	sk := nostr.Generate()
	raw := &slicestore.SliceStore{}
	raw.Init()

	// Stored before search existed, with many events sharing timestamps to
	// exercise paging
	base := nostr.Now() - 1000
	var want []nostr.ID
	for i := range 25 {
		evt := signedEvent(t, sk, nostr.KindTextNote, fmt.Sprintf("archived kombucha note %d", i), base+nostr.Timestamp(i/7))
		raw.SaveEvent(evt)
		want = append(want, evt.ID)
	}
	raw.SaveEvent(signedEvent(t, sk, nostr.KindGiftWrap, "kombucha wrapped", base))
	raw.SaveEvent(signedEvent(t, sk, nostr.KindReaction, "kombucha", base))

	s, err := newSearchStore(raw, t.TempDir()+"/search")
	if err != nil {
		t.Fatal(err)
	}
	defer s.index.Close()
	searchIndex = s
	defer func() { searchIndex = nil }()

	if got := slices.Collect(s.QueryEvents(nostr.Filter{Search: "kombucha"}, 100)); len(got) != 0 {
		t.Fatalf("index not empty before migration: %d", len(got))
	}

	indexed, err := s.indexExisting(5)
	if err != nil {
		t.Fatal(err)
	}
	if indexed != len(want) {
		t.Fatalf("indexed %d, want %d", indexed, len(want))
	}
	got := ids(slices.Collect(s.QueryEvents(nostr.Filter{Search: "kombucha"}, 100)))
	if len(got) != len(want) {
		t.Fatalf("found %d, want %d", len(got), len(want))
	}
	for _, id := range want {
		if !slices.Contains(got, id) {
			t.Fatalf("missing %s", id)
		}
	}

	// The registered migration runs the same thing
	if err := migration0004IndexSearch(); err != nil {
		t.Fatal(err)
	}
}
