package main

import (
	"errors"
	"iter"
	"strconv"

	"fiatjaf.com/nostr"
	"fiatjaf.com/nostr/eventstore"
	bleve "github.com/blevesearch/bleve/v2"
	bleveMapping "github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
)

// textIndex is a bleve full-text index over event content. It follows
// fiatjaf.com/nostr/eventstore/bleve (same fields, same query), but keeps the
// bleve index reachable so the migration can index in batches: one document
// at a time takes over 10ms each, which is hours on a large archive
type textIndex struct {
	path  string
	raw   eventstore.Store
	index bleve.Index
}

const (
	textContentField   = "c"
	textKindField      = "k"
	textCreatedAtField = "a"
	textPubkeyField    = "p"
)

func openTextIndex(path string, raw eventstore.Store) (*textIndex, error) {
	index, err := bleve.Open(path)
	if errors.Is(err, bleve.ErrorIndexPathDoesNotExist) {
		index, err = bleve.New(path, bleveMapping.NewIndexMapping())
	}
	if err != nil {
		return nil, err
	}
	return &textIndex{path: path, raw: raw, index: index}, nil
}

func (t *textIndex) Close() { t.index.Close() }

func textDoc(event nostr.Event) map[string]any {
	return map[string]any{
		textContentField:   event.Content,
		textKindField:      strconv.Itoa(int(event.Kind)),
		textPubkeyField:    event.PubKey.Hex()[56:],
		textCreatedAtField: float64(event.CreatedAt),
	}
}

func (t *textIndex) SaveEvent(event nostr.Event) error {
	return t.index.Index(event.ID.Hex(), textDoc(event))
}

func (t *textIndex) DeleteEvent(id nostr.ID) error {
	return t.index.Delete(id.Hex())
}

func (t *textIndex) DocCount() (uint64, error) {
	return t.index.DocCount()
}

// textBatch collects documents and writes them in one go
type textBatch struct {
	t     *textIndex
	batch *bleve.Batch
}

func (t *textIndex) newBatch() *textBatch {
	return &textBatch{t: t, batch: t.index.NewBatch()}
}

func (b *textBatch) add(event nostr.Event, flushAt int) error {
	if err := b.batch.Index(event.ID.Hex(), textDoc(event)); err != nil {
		return err
	}
	if b.batch.Size() >= flushAt {
		return b.flush()
	}
	return nil
}

func (b *textBatch) flush() error {
	if b.batch.Size() == 0 {
		return nil
	}
	err := b.t.index.Batch(b.batch)
	b.batch.Reset()
	return err
}

// QueryEvents returns the events matching filter.Search, best match first,
// narrowed by kinds, authors, since and until. Events are read back from the
// raw store, so anything deleted there is skipped
func (t *textIndex) QueryEvents(filter nostr.Filter, maxLimit int) iter.Seq[nostr.Event] {
	return func(yield func(nostr.Event) bool) {
		if tlimit := filter.GetTheoreticalLimit(); tlimit == 0 {
			return
		} else if tlimit < maxLimit {
			maxLimit = tlimit
		}
		if len(filter.Search) < 2 {
			return
		}

		searchQ := bleve.NewMatchQuery(filter.Search)
		searchQ.SetField(textContentField)
		conj := []query.Query{searchQ}

		if len(filter.Kinds) > 0 {
			either := bleve.NewDisjunctionQuery()
			for _, kind := range filter.Kinds {
				q := bleve.NewTermQuery(strconv.Itoa(int(kind)))
				q.SetField(textKindField)
				either.AddQuery(q)
			}
			conj = append(conj, either)
		}
		if len(filter.Authors) > 0 {
			either := bleve.NewDisjunctionQuery()
			for _, pk := range filter.Authors {
				q := bleve.NewTermQuery(pk.Hex()[56:])
				q.SetField(textPubkeyField)
				either.AddQuery(q)
			}
			conj = append(conj, either)
		}
		if filter.Since != 0 || filter.Until != 0 {
			var lo, hi *float64
			if filter.Since != 0 {
				v := float64(filter.Since)
				lo = &v
			}
			if filter.Until != 0 {
				v := float64(filter.Until)
				hi = &v
			}
			q := bleve.NewNumericRangeInclusiveQuery(lo, hi, nil, nil)
			q.SetField(textCreatedAtField)
			conj = append(conj, q)
		}

		var q query.Query = searchQ
		if len(conj) > 1 {
			q = bleve.NewConjunctionQuery(conj...)
		}
		req := bleve.NewSearchRequest(q)
		req.Size = maxLimit
		result, err := t.index.Search(req)
		if err != nil {
			return
		}
		for _, hit := range result.Hits {
			id, err := nostr.IDFromHex(hit.ID)
			if err != nil {
				continue
			}
			for event := range t.raw.QueryEvents(nostr.Filter{IDs: []nostr.ID{id}}, 1) {
				if !yield(event) {
					return
				}
			}
		}
	}
}
