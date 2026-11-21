package main

import (
	"context"
	"iter"
	"slices"

	"fiatjaf.com/nostr"
	"fiatjaf.com/nostr/eventstore"
	"fiatjaf.com/nostr/khatru"
	"fiatjaf.com/nostr/khatru/policies"
)

// Only public, human-written text is indexed. Restricted kinds (gift wraps,
// Manent notes) and anything else end-to-end encrypted are never indexed
var searchableKinds = []nostr.Kind{nostr.KindProfileMetadata, nostr.KindTextNote, nostr.KindArticle}

func isSearchableKind(kind nostr.Kind) bool {
	return slices.Contains(searchableKinds, kind) && !isRestrictedKind(kind)
}

// searchStore wraps the main event store with a NIP-50 full-text index.
// The bleve index only holds ids and searchable fields, the events themselves
// are always read back from the main store
type searchStore struct {
	eventstore.Store
	index *textIndex
}

var searchIndex *searchStore

func newSearchStore(main eventstore.Store, path string) (*searchStore, error) {
	index, err := openTextIndex(path, main)
	if err != nil {
		return nil, err
	}
	return &searchStore{Store: main, index: index}, nil
}

func (s *searchStore) Close() {
	s.index.Close()
	s.Store.Close()
}

func (s *searchStore) SaveEvent(event nostr.Event) error {
	if err := s.Store.SaveEvent(event); err != nil {
		return err
	}
	if isSearchableKind(event.Kind) {
		return s.index.SaveEvent(event)
	}
	return nil
}

func (s *searchStore) ReplaceEvent(event nostr.Event) error {
	if err := s.Store.ReplaceEvent(event); err != nil {
		return err
	}
	if !isSearchableKind(event.Kind) {
		return nil
	}
	// Keep only the version the main store kept: it already decided which one wins
	filter := nostr.Filter{Kinds: []nostr.Kind{event.Kind}, Authors: []nostr.PubKey{event.PubKey}}
	if event.Kind.IsAddressable() {
		filter.Tags = nostr.TagMap{"d": []string{event.Tags.GetD()}}
	}
	for current := range s.Store.QueryEvents(filter, 1) {
		if current.ID != event.ID {
			return nil
		}
	}
	return s.index.SaveEvent(event)
}

func (s *searchStore) DeleteEvent(id nostr.ID) error {
	if err := s.Store.DeleteEvent(id); err != nil {
		return err
	}
	return s.index.DeleteEvent(id)
}

func (s *searchStore) QueryEvents(filter nostr.Filter, maxLimit int) iter.Seq[nostr.Event] {
	if filter.Search == "" {
		return s.Store.QueryEvents(filter, maxLimit)
	}
	// bleve only applies search, kinds, authors and dates, so the rest of the
	// filter (ids, tags) is checked here, and restricted kinds are dropped even
	// though they are never indexed
	rest := filter
	rest.Search = ""
	return func(yield func(nostr.Event) bool) {
		for event := range s.index.QueryEvents(filter, maxLimit) {
			if isRestrictedKind(event.Kind) || !rest.Matches(event) {
				continue
			}
			if !yield(event) {
				return
			}
		}
	}
}

// indexExisting indexes every searchable event already in the main store,
// paging backwards in time. Indexing the same id twice just overwrites it
func (s *searchStore) indexExisting(pageSize int) (int, error) {
	indexed := 0
	batch := s.index.newBatch()
	index := func(event nostr.Event) error {
		indexed++
		return batch.add(event, 500)
	}
	for _, kind := range searchableKinds {
		var until nostr.Timestamp
		for {
			filter := nostr.Filter{Kinds: []nostr.Kind{kind}, Until: until}
			var oldest nostr.Timestamp
			page := 0
			done := map[nostr.ID]bool{}
			for event := range s.Store.QueryEvents(filter, pageSize) {
				page++
				if oldest == 0 || event.CreatedAt < oldest {
					oldest = event.CreatedAt
				}
				if err := index(event); err != nil {
					return indexed, err
				}
				done[event.ID] = true
			}
			if page < pageSize {
				break
			}
			// The page may have cut through the oldest second: take all of it,
			// then continue strictly before it
			for event := range s.Store.QueryEvents(nostr.Filter{Kinds: []nostr.Kind{kind}, Until: oldest}, 1<<20) {
				if event.CreatedAt < oldest {
					break
				}
				if !done[event.ID] {
					if err := index(event); err != nil {
						return indexed, err
					}
				}
			}
			if oldest <= 1 {
				break
			}
			until = oldest - 1
		}
	}
	return indexed, batch.flush()
}

// searchOwnerOnly refuses NIP-50 search to everyone but the authenticated
// owner: single threads are public, but searching the archive profiles the owner
func searchOwnerOnly(ownerPubkey nostr.PubKey) func(context.Context, nostr.Filter) (bool, string) {
	return func(ctx context.Context, filter nostr.Filter) (bool, string) {
		if filter.Search == "" {
			return false, ""
		}
		if _, isAuthed := khatru.GetAuthed(ctx); !isAuthed {
			return true, "auth-required: search requires authentication"
		}
		if !khatru.IsAuthed(ctx, ownerPubkey) {
			return true, "restricted: search is only available to the relay owner"
		}
		return false, ""
	}
}

// noEmptyFiltersExceptSearch is policies.NoEmptyFilters, but lets a filter with
// only a search term through: searchOwnerOnly has already limited it to the owner
func noEmptyFiltersExceptSearch(ctx context.Context, filter nostr.Filter) (bool, string) {
	if filter.Search != "" {
		return false, ""
	}
	return policies.NoEmptyFilters(ctx, filter)
}
