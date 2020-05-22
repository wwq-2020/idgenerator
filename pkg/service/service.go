package service

import "context"

// Service Service
type Service interface {
	NewID(ctx context.Context, req *NewIDRequest) (*NewIDResp, error)
}

// Mode Mode
type Mode string

const (
	// ModeStrict ModeStrict
	ModeStrict Mode = "strict"
	// ModeTrend ModeTrend
	ModeTrend Mode = "trend"
)

// NewIDRequest NewIDRequest
type NewIDRequest struct {
	Biz  string `json:"biz"`
	Mode Mode   `json:"mode"`
}

// NewIDResp NewIDResp
type NewIDResp struct {
	ID int64 `json:"id"`
}

// NewIDFunc NewIDFunc
type NewIDFunc func(ctx context.Context, req *NewIDRequest) (*NewIDResp, error)
