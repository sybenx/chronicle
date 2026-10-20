package main

import (
	"log"

	"fiatjaf.com/nostr"
)

func init() {
	registerMigration(3, "0003RebuildRootNotes", migration0003RebuildRootNotes)
}

// migration0003RebuildRootNotes replays the owner's stored events through
// addEventToRootList, restoring threads lost when the old non-atomic save left
// root_notes truncated. It only adds entries, never removes them
func migration0003RebuildRootNotes() error {
	ownerPK, err := nostr.PubKeyFromHex(config.OwnerPubkey)
	if err != nil {
		return err
	}

	before := rootNotesList.Size()
	events := 0
	for event := range store.QueryEvents(nostr.Filter{
		Authors: []nostr.PubKey{ownerPK},
		Kinds: []nostr.Kind{
			nostr.KindTextNote,
			nostr.KindArticle,
			nostr.KindReaction,
			nostr.KindZapRequest,
		},
	}, maxOwnerEvents) {
		addEventToRootList(event)
		events++
	}

	updateExternalThreadStates()

	log.Printf("🧵 Rebuilt root notes from %d owner events: %d -> %d threads", events, before, rootNotesList.Size())
	return rootNotesList.SaveToFile()
}
