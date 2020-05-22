package idfetcher

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	etcd "go.etcd.io/etcd/clientv3"

	"github.com/wwq1988/idgenerator/pkg/common"
	"github.com/wwq1988/idgenerator/pkg/conf"
	"github.com/wwq1988/idgenerator/pkg/leadership"
)

// IDFetcher IDFetcher
type IDFetcher interface {
	NextIDRange(step int) []int64
}

type idFetcher struct {
	etcd           *etcd.Client
	lock           sync.Mutex
	isLeader       uint32
	CurIDKey       string
	leaderStatus   uint32
	shouldBeLeader bool
}

// Factory Factory
type Factory func(leadership leadership.Leadership, biz string) IDFetcher

// CreateFactory CreateFactory
func CreateFactory(conf *conf.KV, etcd *etcd.Client, shouldBeLeader bool) Factory {
	return func(leadership leadership.Leadership, biz string) IDFetcher {
		return New(conf, etcd, leadership, biz, shouldBeLeader)
	}
}

// New New
func New(conf *conf.KV, etcd *etcd.Client, leadership leadership.Leadership, biz string, shouldBeLeader bool) IDFetcher {
	f := &idFetcher{
		etcd:           etcd,
		CurIDKey:       fmt.Sprintf(conf.CurIDKeyFmt, biz),
		leaderStatus:   leadership.GetLeaderStatus().ToUint32(),
		shouldBeLeader: shouldBeLeader,
	}
	leadership.OnLeaderStatusChange(f.onLeaderChange)
	return f
}

func (f *idFetcher) onLeaderChange(leaderStatus common.LeaderStatus) {
	atomic.StoreUint32(&f.leaderStatus, leaderStatus.ToUint32())
}

// NextIDRange NextIDRange
func (f *idFetcher) NextIDRange(step int) []int64 {
	revision := int64(0)
	cmp := etcd.CreateRevision(f.CurIDKey)
	for {
		if f.shouldBeLeader && atomic.LoadUint32(&f.leaderStatus) == common.LeaderStatusNotLeader.ToUint32() {
			return nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		txn := f.etcd.Txn(ctx).If(etcd.Compare(cmp, "=", revision))
		txn = txn.Then(etcd.OpPut(f.CurIDKey, fmt.Sprint(step)))
		txn = txn.Else(etcd.OpGet(f.CurIDKey))
		resp, err := txn.Commit()
		if err != nil {
			cancel()
			time.Sleep(time.Millisecond * 100)
			continue
		}
		if resp.Succeeded {
			cancel()
			ids := make([]int64, 0, step)
			for i := 0; i < step; i++ {
				ids = append(ids, int64(i))
			}
			return ids
		}
		cmp = etcd.ModRevision(f.CurIDKey)
		cancel()
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			if f.shouldBeLeader && atomic.LoadUint32(&f.leaderStatus) == common.LeaderStatusNotLeader.ToUint32() {
				cancel()
				return nil
			}
			revision = resp.Responses[0].GetResponseRange().Kvs[0].ModRevision
			value := resp.Responses[0].GetResponseRange().Kvs[0].Value
			start, err := strconv.Atoi(string(value))
			if err != nil {
				panic("unexpected curID val")
			}
			end := start + step
			txn = f.etcd.Txn(ctx).If(etcd.Compare(cmp, "=", revision))
			txn = txn.Then(etcd.OpPut(f.CurIDKey, fmt.Sprint(end)))
			txn = txn.Else(etcd.OpGet(f.CurIDKey))
			resp, err = txn.Commit()
			if err != nil {
				cancel()
				time.Sleep(time.Millisecond * 100)
				continue
			}
			if !resp.Succeeded {
				cancel()
				time.Sleep(time.Millisecond * 100)
				continue
			}
			cancel()
			ids := make([]int64, 0, step)
			for i := start; i < end; i++ {
				ids = append(ids, int64(i))
			}
			return ids
		}
	}
}
