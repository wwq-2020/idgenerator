package idgetter

import "context"

// Factory Factory
type Factory func() IDGetter

// IDGetter IDGetter
type IDGetter interface {
	GetID(ctx context.Context, biz string) (int64, error)
}
