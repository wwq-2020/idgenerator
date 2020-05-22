package common

import (
	"errors"
	"net"
	"net/http"
	"time"
)

var (
	// ErrNotLeader ErrNotLeader
	ErrNotLeader = errors.New("not leader")
)

// LeaderStatus LeaderStatus
type LeaderStatus int

const (
	// LeaderStatusNotLeader LeaderStatusNotLeader
	LeaderStatusNotLeader LeaderStatus = 0
	// LeaderStatusIsLeader LeaderStatusIsLeader
	LeaderStatusIsLeader LeaderStatus = 1
)

// ToUint32 ToUint32
func (ls LeaderStatus) ToUint32() uint32 {
	return uint32(ls)
}

// Client Client
var Client = &http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 15 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
}
