package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"fiatjaf.com/nostr"
)

// guardBlossomWrites rejects uploads and mirrors from anyone but the owner
// before khatru's blossom handlers run: upstream allocates a buffer sized by
// Content-Length (upload) or downloads an arbitrary URL into memory (mirror)
// before calling RejectUpload, so any keypair could trigger them
func guardBlossomWrites(next http.Handler, ownerPubkey nostr.PubKey) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && (r.URL.Path == "/upload" || r.URL.Path == "/mirror") {
			auth, ok := readBlossomAuth(r)
			if !ok {
				http.Error(w, "missing or invalid \"Authorization\" header", http.StatusUnauthorized)
				return
			}
			if auth.PubKey != ownerPubkey {
				http.Error(w, "Not allowed", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// readBlossomAuth mirrors the checks of khatru's blossom readAuthorization
func readBlossomAuth(r *http.Request) (nostr.Event, bool) {
	var evt nostr.Event
	token, found := strings.CutPrefix(r.Header.Get("Authorization"), "Nostr ")
	if !found {
		return evt, false
	}
	eventj, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return evt, false
	}
	if err := json.Unmarshal(eventj, &evt); err != nil {
		return evt, false
	}
	if evt.Kind != 24242 || !evt.CheckID() || !evt.VerifySignature() {
		return evt, false
	}
	expirationTag := evt.Tags.Find("expiration")
	if expirationTag == nil {
		return evt, false
	}
	expiration, _ := strconv.ParseInt(expirationTag[1], 10, 64)
	if nostr.Timestamp(expiration) < nostr.Now() {
		return evt, false
	}
	return evt, true
}
