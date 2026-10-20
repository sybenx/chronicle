package main

import (
	"context"
	"iter"
	"slices"

	"fiatjaf.com/nostr"
	"fiatjaf.com/nostr/khatru"
)

// Gift wraps and Manent private notes are only served to the authenticated owner
var restrictedKinds = []nostr.Kind{nostr.KindGiftWrap, 33301, 33302}

func isRestrictedKind(kind nostr.Kind) bool {
	return slices.Contains(restrictedKinds, kind)
}

func filterMayMatchRestricted(filter nostr.Filter) bool {
	if len(filter.Kinds) == 0 {
		return true
	}
	for _, kind := range filter.Kinds {
		if isRestrictedKind(kind) {
			return true
		}
	}
	return false
}

// protectRestrictedEvents enforces the owner-only rule on every event the relay
// returns, instead of trusting the kinds listed in the filter: a filter like
// {"#p":[owner]} or {"authors":[owner]} would otherwise match restricted events
func protectRestrictedEvents(relay *khatru.Relay, ownerPubkey nostr.PubKey) {
	queryStored := relay.QueryStored
	relay.QueryStored = func(ctx context.Context, filter nostr.Filter) iter.Seq[nostr.Event] {
		events := queryStored(ctx, filter)
		// Deletion and expiration run as internal calls and must see everything
		if khatru.IsInternalCall(ctx) || khatru.IsAuthed(ctx, ownerPubkey) {
			return events
		}
		return func(yield func(nostr.Event) bool) {
			for event := range events {
				if isRestrictedKind(event.Kind) {
					continue
				}
				if !yield(event) {
					return
				}
			}
		}
	}

	relay.OnCount = func(ctx context.Context, filter nostr.Filter) (bool, string) {
		if filterMayMatchRestricted(filter) && !khatru.IsAuthed(ctx, ownerPubkey) {
			return true, "restricted: counting these events requires owner authentication"
		}
		return false, ""
	}

	relay.PreventBroadcast = func(ws *khatru.WebSocket, filter nostr.Filter, event nostr.Event) bool {
		return isRestrictedKind(event.Kind) && !slices.Contains(ws.AuthedPublicKeys, ownerPubkey)
	}
}
