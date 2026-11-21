package main

import "log"

func init() {
	registerMigration(4, "0004IndexSearch", migration0004IndexSearch)
}

// migration0004IndexSearch builds the NIP-50 index from the profiles, notes and
// articles already stored. New events are indexed as they are saved
func migration0004IndexSearch() error {
	indexed, err := searchIndex.indexExisting(1000)
	if err != nil {
		return err
	}
	log.Printf("🔎 Indexed %d stored events for search", indexed)
	return nil
}
