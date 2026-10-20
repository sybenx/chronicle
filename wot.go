package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"fiatjaf.com/nostr"
	"fiatjaf.com/nostr/khatru"
)

// The trust network is rebuilt from scratch on every refresh and swapped in
// under wotMu, since event handlers read it concurrently
var (
	wotMu           sync.RWMutex
	trustNetwork    []string
	trustNetworkMap = make(map[string]bool)
	relays          []string
)

func belongsToWotNetwork(event nostr.Event) bool {
	pubkey := event.PubKey
	// Zap receipts are signed by the LNURL server, the sender is in the embedded zap request
	if event.Kind == nostr.KindZap {
		sender, ok := zapSender(event)
		if !ok {
			return false
		}
		pubkey = sender
	}
	return isTrusted(pubkey.Hex())
}

func isTrusted(pubkey string) bool {
	wotMu.RLock()
	defer wotMu.RUnlock()
	return trustNetworkMap[pubkey]
}

// zapSender returns the signer of the zap request embedded in a zap receipt
func zapSender(receipt nostr.Event) (nostr.PubKey, bool) {
	description := receipt.Tags.Find("description")
	if description == nil {
		return nostr.PubKey{}, false
	}
	var request nostr.Event
	if err := json.Unmarshal([]byte(description[1]), &request); err != nil {
		return nostr.PubKey{}, false
	}
	if request.Kind != nostr.KindZapRequest || !request.CheckID() || !request.VerifySignature() {
		return nostr.PubKey{}, false
	}
	return request.PubKey, true
}

func trustNetworkSnapshot() []string {
	wotMu.RLock()
	defer wotMu.RUnlock()
	return append([]string(nil), trustNetwork...)
}

// initTrustNetwork seeds the trust network with the owner, so the owner is
// trusted even before the first refresh completes
func initTrustNetwork(ownerPubkey string) {
	wotMu.Lock()
	defer wotMu.Unlock()
	trustNetwork = []string{ownerPubkey}
	trustNetworkMap = map[string]bool{ownerPubkey: true}
}

func refreshProfiles(ctx context.Context, relay *khatru.Relay) {
	network := trustNetworkSnapshot()
	for i := 0; i < len(network); i += 200 {
		end := min(i+200, len(network))
		filter := nostr.Filter{
			Authors: pubKeysFromHexes(network[i:end]),
			Kinds:   []nostr.Kind{nostr.KindProfileMetadata},
		}

		timeout, cancel := context.WithTimeout(ctx, 4*time.Second)
		for ev := range relayPool().FetchMany(timeout, seedRelays, filter, nostr.SubscriptionOptions{}) {
			relay.AddEvent(ctx, ev.Event)
		}
		cancel()
	}
	log.Println("👤 WoT: profiles refreshed: ", len(network))
}

// latestPerAuthor keeps only the newest event per author, so replaceable
// events returned by several relays (or in several versions) count once
func latestPerAuthor(latest map[nostr.PubKey]nostr.Event, ev nostr.Event) {
	if prev, ok := latest[ev.PubKey]; !ok || ev.CreatedAt > prev.CreatedAt {
		latest[ev.PubKey] = ev
	}
}

func refreshTrustNetwork(ctx context.Context, relay *khatru.Relay) {
	ownerPK := nostr.MustPubKeyFromHex(config.OwnerPubkey)
	filter := nostr.Filter{
		Authors: []nostr.PubKey{ownerPK},
		Kinds:   []nostr.Kind{nostr.KindFollowList},
	}

	log.Println("🔍 WoT: fetching owner's follows")
	ownerLists := make(map[nostr.PubKey]nostr.Event)
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	for ev := range relayPool().FetchMany(timeoutCtx, seedRelays, filter, nostr.SubscriptionOptions{}) {
		latestPerAuthor(ownerLists, ev.Event)
	}
	cancel()

	ownerList, ok := ownerLists[ownerPK]
	if !ok {
		log.Println("⚠️  WoT: owner's follow list not found, keeping the current trust network")
		return
	}

	followerCount := make(map[string]int)
	var oneHopNetwork []string
	oneHopSeen := make(map[string]bool)
	for contact := range ownerList.Tags.FindAll("p") {
		if len(contact[1]) != 64 || oneHopSeen[contact[1]] {
			continue
		}
		oneHopSeen[contact[1]] = true
		oneHopNetwork = append(oneHopNetwork, contact[1])
		followerCount[contact[1]]++
	}

	log.Println("🌐 WoT: building web of trust graph...")
	followLists := make(map[nostr.PubKey]nostr.Event)
	relayLists := make(map[nostr.PubKey]nostr.Event)
	for i := 0; i < len(oneHopNetwork); i += 100 {
		end := min(i+100, len(oneHopNetwork))
		filter = nostr.Filter{
			Authors: pubKeysFromHexes(oneHopNetwork[i:end]),
			Kinds:   []nostr.Kind{nostr.KindFollowList, nostr.KindRelayListMetadata, nostr.KindProfileMetadata},
		}

		timeout, cancel := context.WithTimeout(ctx, 4*time.Second)
		for ev := range relayPool().FetchMany(timeout, seedRelays, filter, nostr.SubscriptionOptions{}) {
			switch ev.Kind {
			case nostr.KindFollowList:
				latestPerAuthor(followLists, ev.Event)
			case nostr.KindRelayListMetadata:
				latestPerAuthor(relayLists, ev.Event)
			case nostr.KindProfileMetadata:
				relay.AddEvent(ctx, ev.Event)
			}
		}
		cancel()
	}

	for _, list := range followLists {
		// A follow list may repeat the same contact, count each follower once
		seen := make(map[string]bool)
		for contact := range list.Tags.FindAll("p") {
			if seen[contact[1]] {
				continue
			}
			seen[contact[1]] = true
			followerCount[contact[1]]++
		}
	}

	var newRelays []string
	relaySeen := make(map[string]bool)
	for _, list := range relayLists {
		for r := range list.Tags.FindAll("r") {
			if !relaySeen[r[1]] {
				relaySeen[r[1]] = true
				newRelays = append(newRelays, r[1])
			}
		}
	}

	newNetwork := []string{config.OwnerPubkey}
	newMap := map[string]bool{config.OwnerPubkey: true}
	for pubkey, count := range followerCount {
		if count >= config.MinFollowers && len(pubkey) == 64 && !newMap[pubkey] {
			newMap[pubkey] = true
			newNetwork = append(newNetwork, pubkey)
		}
	}

	wotMu.Lock()
	trustNetwork = newNetwork
	trustNetworkMap = newMap
	relays = newRelays
	wotMu.Unlock()

	log.Println("🫂  WoT: total network size:", len(followerCount))
	log.Println("🔗 WoT: relays discovered:", len(newRelays))
	log.Println("🌐 WoT: trust network updated with", len(newNetwork), "keys")
}
