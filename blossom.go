package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"fiatjaf.com/nostr"
)

var sha256HexRegex = regexp.MustCompile(`^[a-fA-F0-9]{64}$`)
var blossomURLRegex = regexp.MustCompile(`https?://[^/\s]+/([a-fA-F0-9]{64})(?:\.[a-zA-Z0-9]+)?`)
var ownerBlossomTrackingFile string
var ownerBlossomMutex sync.Mutex

func initOwnerBlossomTracking() {
	ownerBlossomTrackingFile = filepath.Join(config.DBPath, "owner_blossom")

	if _, err := os.Stat(ownerBlossomTrackingFile); os.IsNotExist(err) {
		log.Println("🗂️  Bootstrapping owner Blossom file tracking")
		bootstrapOwnerBlossomFiles()
	} else {
		log.Println("🗂️  Owner Blossom tracking file found")
	}
}

func bootstrapOwnerBlossomFiles() {
	hashSet := make(map[string]bool)

	filesFound := 0
	if entries, err := os.ReadDir(config.BlossomAssetsPath); err == nil {
		for _, entry := range entries {
			if !entry.IsDir() {
				filename := entry.Name()
				if sha256HexRegex.MatchString(filename) {
					hashSet[filename] = true
					filesFound++
				}
			}
		}
		log.Printf("🗂️  Found %d existing owner Blossom files in assets directory", filesFound)
	} else {
		log.Printf("Warning: Could not read assets directory: %v", err)
	}

	ownerPK, err := nostr.PubKeyFromHex(config.OwnerPubkey)
	if err != nil {
		log.Printf("Error parsing owner pubkey: %v", err)
		return
	}

	missingFiles := make(map[string][]string)
	eventHashes := 0
	eventsWithHashes := 0

	for event := range store.QueryEvents(nostr.Filter{Authors: []nostr.PubKey{ownerPK}}, maxOwnerEvents) {
		hashes := extractBlossomHashes(event.Content)
		if len(hashes) > 0 {
			eventsWithHashes++
		}
		for _, hash := range hashes {
			eventHashes++
			if !hashSet[hash] {
				matches := blossomURLRegex.FindAllString(event.Content, -1)
				for _, url := range matches {
					if strings.Contains(url, hash) {
						missingFiles[hash] = append(missingFiles[hash], url)
						break
					}
				}
			}
			hashSet[hash] = true
		}
	}

	log.Printf("🔍 Found %d Blossom hashes in %d owner events", eventHashes, eventsWithHashes)

	downloaded := 0
	failed := 0

	for hash, urls := range missingFiles {
		for _, url := range urls {
			if err := downloadBlossomFile(url, hash, noSizeLimit); err == nil {
				downloaded++
				log.Printf("✅ Downloaded missing owner file: %s", hash[:16]+"...")
				break
			} else {
				failed++
				log.Printf("❌ Failed to download owner file: %s - %s", url, err)
			}
		}
	}

	if len(missingFiles) > 0 {
		log.Printf("📥 Download results: %d succeeded, %d failed", downloaded, failed)
	}

	file, err := os.Create(ownerBlossomTrackingFile)
	if err != nil {
		log.Printf("Error creating owner Blossom tracking file: %v", err)
		return
	}
	defer file.Close()

	for hash := range hashSet {
		file.WriteString(hash + "\n")
	}

	log.Printf("📝 Bootstrapped %d total owner Blossom files (%d existing + %d from events)", len(hashSet), filesFound, len(hashSet)-filesFound)
}

func extractBlossomHashes(content string) []string {
	matches := blossomURLRegex.FindAllStringSubmatch(content, -1)
	var hashes []string
	for _, match := range matches {
		if len(match) > 1 {
			hashes = append(hashes, match[1])
		}
	}
	return hashes
}

func trackOwnerBlossomFile(hash string) {
	ownerBlossomMutex.Lock()
	defer ownerBlossomMutex.Unlock()

	file, err := os.OpenFile(ownerBlossomTrackingFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Error opening owner Blossom tracking file: %v", err)
		return
	}
	defer file.Close()

	if _, err := file.WriteString(hash + "\n"); err != nil {
		log.Printf("Error writing to owner Blossom tracking file: %v", err)
	}
}

func isFileAlreadyDownloaded(hash string) bool {
	filePath := filepath.Join(config.BlossomAssetsPath, hash)
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

// downloadBlossomFile fetches url and stores it under hash once the content
// hashes to it. A negative maxBytes (noSizeLimit) means no size limit
func downloadBlossomFile(rawURL, hash string, maxBytes int64) error {
	if isFileAlreadyDownloaded(hash) {
		return nil
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("unsupported scheme %q", parsed.Scheme)
	}

	ctx, cancel := mediaContext(maxBytes)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return err
	}

	resp, err := mediaHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	if maxBytes >= 0 && resp.ContentLength > maxBytes {
		return fmt.Errorf("file too large: %d bytes (max %d MB)", resp.ContentLength, maxBytes/(1024*1024))
	}

	// A unique temp name, so concurrent downloads of the same hash don't collide
	file, err := os.CreateTemp(config.BlossomAssetsPath, hash+".*.tmp")
	if err != nil {
		return err
	}
	tempFile := file.Name()
	defer func() {
		file.Close()
		os.Remove(tempFile)
	}()

	hasher := sha256.New()
	var body io.Reader = resp.Body
	if maxBytes >= 0 {
		body = io.LimitReader(resp.Body, maxBytes+1)
	}

	written, err := io.Copy(file, io.TeeReader(body, hasher))
	if err != nil {
		return err
	}
	if maxBytes >= 0 && written > maxBytes {
		return fmt.Errorf("file too large: %d bytes (max %d MB)", written, maxBytes/(1024*1024))
	}

	calculatedHash := fmt.Sprintf("%x", hasher.Sum(nil))
	if calculatedHash != hash {
		return fmt.Errorf("hash mismatch: expected %s, got %s", hash, calculatedHash)
	}

	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tempFile, 0644); err != nil {
		return err
	}

	finalPath := filepath.Join(config.BlossomAssetsPath, hash)
	if err := os.Rename(tempFile, finalPath); err != nil {
		return err
	}

	log.Printf("📥 Downloaded Blossom file: %s (%d bytes)", hash[:16]+"...", written)
	return nil
}

func getUserWriteRelays(authorPubkey string) []string {
	ctx := context.Background()
	timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	pk, err := nostr.PubKeyFromHex(authorPubkey)
	if err != nil {
		return nil
	}

	filter := nostr.Filter{
		Authors: []nostr.PubKey{pk},
		Kinds:   []nostr.Kind{nostr.KindRelayListMetadata},
		Limit:   1,
	}

	var relayListEvent nostr.Event
	var found bool
	for event := range store.QueryEvents(filter, 1) {
		if !found || event.CreatedAt > relayListEvent.CreatedAt {
			relayListEvent = event
			found = true
		}
	}

	if !found {
		for ev := range relayPool().FetchMany(timeout, seedRelays, filter, nostr.SubscriptionOptions{}) {
			if !found || ev.CreatedAt > relayListEvent.CreatedAt {
				relayListEvent = ev.Event
				found = true
				saveEvent(ev.Event)
			}
		}
	}

	if !found {
		return nil
	}

	var writeRelays []string
	for tag := range relayListEvent.Tags.FindAll("r") {
		if len(tag) >= 2 && tag[1] != "" {
			if len(tag) < 3 || tag[2] == "" || tag[2] == "write" {
				writeRelays = append(writeRelays, tag[1])
			}
		}
	}

	return writeRelays
}

func tryDownloadFromServers(servers []string, hash string, maxBytes int64) bool {
	for _, server := range servers {
		serverURL := fmt.Sprintf("%s/%s", strings.TrimRight(server, "/"), hash)
		if err := downloadBlossomFile(serverURL, hash, maxBytes); err == nil {
			log.Printf("✅ Downloaded from server: %s", server)
			return true
		}
	}
	return false
}

func downloadFromAltBlossomServers(authorPubkey string, hash string, maxBytes int64) bool {
	ctx := context.Background()
	timeout, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	pk, err := nostr.PubKeyFromHex(authorPubkey)
	if err != nil {
		return false
	}

	serverFilter := nostr.Filter{
		Authors: []nostr.PubKey{pk},
		Kinds:   []nostr.Kind{10063},
		Limit:   1,
	}

	var serverListEvent nostr.Event
	var found bool
	for event := range store.QueryEvents(serverFilter, 1) {
		if !found || event.CreatedAt > serverListEvent.CreatedAt {
			serverListEvent = event
			found = true
		}
	}

	if found {
		servers := extractServersFromEvent(serverListEvent)
		if len(servers) > 0 {
			log.Printf("🔍 Trying servers from local DB for %s", authorPubkey[:8]+"...")
			if tryDownloadFromServers(servers, hash, maxBytes) {
				return true
			}
		}
	}

	found = false
	for ev := range relayPool().FetchMany(timeout, seedRelays, serverFilter, nostr.SubscriptionOptions{}) {
		if !found || ev.CreatedAt > serverListEvent.CreatedAt {
			serverListEvent = ev.Event
			found = true
		}
	}

	if found {
		saveEvent(serverListEvent)
		servers := extractServersFromEvent(serverListEvent)
		if len(servers) > 0 {
			log.Printf("🔍 Trying servers from seedRelays for %s", authorPubkey[:8]+"...")
			if tryDownloadFromServers(servers, hash, maxBytes) {
				return true
			}
		}
	}

	writeRelays := getUserWriteRelays(authorPubkey)
	if len(writeRelays) > 0 {
		log.Printf("🔍 Searching user's write relays for %s server list", authorPubkey[:8]+"...")

		found = false
		for ev := range relayPool().FetchMany(timeout, writeRelays, serverFilter, nostr.SubscriptionOptions{}) {
			if !found || ev.CreatedAt > serverListEvent.CreatedAt {
				serverListEvent = ev.Event
				found = true
			}
		}

		if found {
			saveEvent(serverListEvent)
			servers := extractServersFromEvent(serverListEvent)
			if len(servers) > 0 {
				log.Printf("🔍 Trying servers from user's write relays for %s", authorPubkey[:8]+"...")
				if tryDownloadFromServers(servers, hash, maxBytes) {
					return true
				}
			}
		}
	}

	return false
}

func extractServersFromEvent(event nostr.Event) []string {
	var servers []string
	for tag := range event.Tags.FindAll("server") {
		if len(tag) >= 2 && tag[1] != "" {
			servers = append(servers, tag[1])
		}
	}
	return servers
}

const noSizeLimit int64 = -1

// reactedTargets holds the IDs of events the owner reacted to or zapped, whose
// media gets the larger ReactedMaxFileSizeMB cap
var reactedTargets sync.Map

func markReactedTarget(id nostr.ID) {
	reactedTargets.Store(id, struct{}{})
}

func mediaSizeLimit(event nostr.Event) int64 {
	if event.PubKey.Hex() == config.OwnerPubkey {
		return noSizeLimit
	}
	if _, ok := reactedTargets.Load(event.ID); ok {
		return int64(config.ReactedMaxFileSizeMB) * 1024 * 1024
	}
	return int64(config.MaxFileSizeMB) * 1024 * 1024
}

// mediaRef is one blob an event points to, with every URL that names it
type mediaRef struct {
	hash string
	urls []string
}

// extractMediaRefs collects Blossom blobs from the content and from NIP-92
// imeta tags, whose url may not carry the hash but whose x field does
func extractMediaRefs(event nostr.Event) []mediaRef {
	var refs []mediaRef
	index := make(map[string]int)
	add := func(hash string, urls ...string) {
		hash = strings.ToLower(hash)
		i, ok := index[hash]
		if !ok {
			i = len(refs)
			index[hash] = i
			refs = append(refs, mediaRef{hash: hash})
		}
		for _, u := range urls {
			if u != "" && !slices.Contains(refs[i].urls, u) {
				refs[i].urls = append(refs[i].urls, u)
			}
		}
	}

	for _, match := range blossomURLRegex.FindAllStringSubmatch(event.Content, -1) {
		add(match[1], match[0])
	}

	for tag := range event.Tags.FindAll("imeta") {
		var hash string
		var urls []string
		for _, field := range tag[1:] {
			key, value, ok := strings.Cut(field, " ")
			if !ok {
				continue
			}
			value = strings.TrimSpace(value)
			switch key {
			case "x":
				if sha256HexRegex.MatchString(value) {
					hash = value
				}
			case "url", "fallback":
				urls = append(urls, value)
			}
		}
		if hash == "" {
			for _, u := range urls {
				if match := blossomURLRegex.FindStringSubmatch(u); match != nil {
					hash = match[1]
					break
				}
			}
		}
		if hash != "" {
			add(hash, urls...)
		}
	}

	return refs
}

func processBlossomBackup(event nostr.Event) {
	if !config.BackupBlossomMedia {
		return
	}

	refs := extractMediaRefs(event)
	if len(refs) == 0 {
		return
	}

	go func() {
		maxBytes := mediaSizeLimit(event)

		for _, ref := range refs {
			if isFileAlreadyDownloaded(ref.hash) {
				continue
			}

			downloaded := false
			for _, u := range ref.urls {
				err := downloadBlossomFile(u, ref.hash, maxBytes)
				if err == nil {
					downloaded = true
					break
				}
				log.Printf("🔄 %s failed: %v", u, err)
			}
			if downloaded {
				continue
			}

			log.Printf("🔄 Trying author's fallback servers for %s", ref.hash[:16]+"...")

			if !downloadFromAltBlossomServers(event.PubKey.Hex(), ref.hash, maxBytes) {
				log.Printf("⚠️  Failed to download Blossom file %s from all available sources", ref.hash[:16]+"...")
			}
		}
	}()
}

// writeFileAtomic writes through a temp file in the same directory and renames
// it into place, so readers never see a partially written blob
func writeFileAtomic(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Chmod(file.Name(), 0644); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}
