package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"syscall"
	"time"
)

// Media URLs come from other people's events, so the backup downloader must
// not be usable to reach the relay host, its LAN or its tailnet
var errNonPublicAddress = errors.New("refusing to connect to a non-public address")

var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), // CGNAT, used by Tailscale
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"), // NAT64 can map back to private IPv4
	netip.MustParsePrefix("2001:db8::/32"),
}

func isPublicAddr(addr netip.Addr) bool {
	addr = addr.Unmap()
	if !addr.IsValid() || addr.IsLoopback() || addr.IsPrivate() || addr.IsUnspecified() ||
		addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() ||
		addr.IsInterfaceLocalMulticast() || addr.IsMulticast() {
		return false
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}

// The check runs on the resolved address at connect time, so DNS rebinding
// and redirects to internal hosts are caught too
func publicOnlyControl(network, address string, _ syscall.RawConn) error {
	addrPort, err := netip.ParseAddrPort(address)
	if err != nil {
		return err
	}
	if !isPublicAddr(addrPort.Addr()) {
		return fmt.Errorf("%w: %s", errNonPublicAddress, addrPort.Addr())
	}
	return nil
}

var mediaHTTPClient = &http.Client{
	Transport: &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
			Control:   publicOnlyControl,
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConns:          20,
	},
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return errors.New("too many redirects")
		}
		if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
			return fmt.Errorf("redirect to unsupported scheme %q", req.URL.Scheme)
		}
		return nil
	},
}

// mediaDownloadTimeout bounds a whole download, scaled to the size cap so
// large files from slow servers still have time to finish
func mediaDownloadTimeout(maxBytes int64) time.Duration {
	if maxBytes <= 0 {
		return 30 * time.Minute
	}
	// Allow at least ~1 MB per 2 seconds, never under 30 seconds
	timeout := time.Duration(maxBytes/(512*1024)) * time.Second
	return max(timeout, 30*time.Second)
}

func mediaContext(maxBytes int64) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), mediaDownloadTimeout(maxBytes))
}
