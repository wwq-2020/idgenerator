package leadership

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/wwq1988/idgenerator/pkg/common"
	"github.com/wwq1988/idgenerator/pkg/conf"
	etcd "go.etcd.io/etcd/clientv3"
	concurrency "go.etcd.io/etcd/clientv3/concurrency"
	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

// Leadership Leadership
type Leadership interface {
	OnLeaderStatusChange(func(common.LeaderStatus))
	GetLeaderStatus() common.LeaderStatus
	Election()
	Done()
	GetLeaderAddr(ctx context.Context, biz string) (string, error)
}

type leadership struct {
	etcd        *etcd.Client
	once        sync.Once
	LeaseTTL    int
	ElectionKey string
	Val         string
	subscribers []func(common.LeaderStatus)
	sync.Mutex
	LeaderKey     string
	leaderAddr    atomic.Value
	leaderStatus  uint32
	ToElectionKey string
	cancel        func()
	ctx           context.Context
}

// Factory Factory
type Factory func(biz string) Leadership

// CreateFactory CreateFactory
func CreateFactory(val string, conf *conf.KV, etcd *etcd.Client) Factory {
	return func(biz string) Leadership {
		return New(conf, etcd, biz, val)
	}
}

// New New
func New(conf *conf.KV, etcd *etcd.Client, biz string, val string) Leadership {
	ctx, cancel := context.WithCancel(context.Background())
	l := &leadership{
		LeaseTTL:      conf.LeaseTTL,
		etcd:          etcd,
		ElectionKey:   fmt.Sprintf(conf.ElectionKeyFmt, biz),
		Val:           val,
		LeaderKey:     fmt.Sprintf(conf.LeaderKeyFmt, biz),
		ToElectionKey: conf.ToElectionKeyPrefix + "/" + fmt.Sprintf("%s", biz),
		ctx:           ctx,
		cancel:        cancel,
	}
	return l
}

func (l *leadership) Done() {
	l.cancel()
}

func (l *leadership) Election() {
	for {
		shouldStop := l.election()
		if shouldStop {
			return
		}
	}
}

func (l *leadership) election() bool {
	session, err := concurrency.NewSession(l.etcd, concurrency.WithTTL(l.LeaseTTL))
	if err != nil {
		return false
	}
	defer session.Close()

	e := concurrency.NewElection(session, l.ElectionKey)
	if err := e.Campaign(l.ctx, l.Val); err != nil {
		select {
		case <-l.ctx.Done():
			return true
		default:
		}
		return false
	}

	atomic.StoreUint32(&l.leaderStatus, common.LeaderStatusIsLeader.ToUint32())
	l.notify(common.LeaderStatusIsLeader)
	select {
	case <-session.Done():
	case <-l.ctx.Done():
		return true
	}
	atomic.StoreUint32(&l.leaderStatus, common.LeaderStatusNotLeader.ToUint32())
	l.notify(common.LeaderStatusNotLeader)
	return false
}

func (l *leadership) notify(leaderStatus common.LeaderStatus) {
	l.Lock()
	subscribers := l.subscribers
	l.Unlock()
	for _, subscriber := range subscribers {
		subscriber(leaderStatus)
	}

}

func (l *leadership) GetLeaderStatus() common.LeaderStatus {
	return common.LeaderStatus(atomic.LoadUint32(&l.leaderStatus))
}

func (l *leadership) OnLeaderStatusChange(subscriber func(common.LeaderStatus)) {
	l.Lock()
	l.subscribers = append(l.subscribers, subscriber)
	l.Unlock()
	subscriber(l.GetLeaderStatus())
}

func (l *leadership) GetLeaderAddr(ctx context.Context, biz string) (string, error) {
	var err error
	var leaderAddr string
	l.once.Do(func() {
		leaderAddr, err = l.getLeaderAddr(ctx)
		go l.watchLoop()
	})
	if err != nil {
		return "", err
	}
	if leaderAddr != "" {
		l.leaderAddr.Store(leaderAddr)
		return leaderAddr, nil
	}

	leaderAddrObj := l.leaderAddr.Load()
	if leaderAddrObj == nil {
		return "", nil
	}
	return leaderAddrObj.(string), nil
}

func (l *leadership) watchLoop() {
	for {
		shouldStop := l.watch()
		if shouldStop {
			return
		}
	}
}

func (l *leadership) watch() bool {
	session, err := concurrency.NewSession(l.etcd, concurrency.WithTTL(1))
	if err != nil {
		return false
	}
	defer session.Close()

	ctx, cancel := context.WithCancel(l.ctx)
	defer cancel()

	ch := make(chan string)
	go l.observe(ctx, ch)
inner:
	for {
		select {
		case <-l.ctx.Done():
			return true
		case <-session.Done():
			break inner
		case leaderAddr, ok := <-ch:
			if !ok {
				break inner
			}
			l.leaderAddr.Store(leaderAddr)
			continue inner
		}
	}
	return false
}

func (l *leadership) getLeaderAddr(ctx context.Context) (string, error) {
	resp, err := l.etcd.Get(ctx, l.LeaderKey, etcd.WithPrefix())
	if err != nil {
		return "", err
	}

	if len(resp.Kvs) == 0 {
		return "", nil
	}

	return string(resp.Kvs[0].Value), nil
}

func (l *leadership) observe(ctx context.Context, retc chan string) {
	client := l.etcd
	defer close(retc)
	for {
		resp, err := client.Get(ctx, l.LeaderKey, etcd.WithLastCreate()...)
		if err != nil {
			return
		}

		var kv *mvccpb.KeyValue
		var hdr *pb.ResponseHeader

		if len(resp.Kvs) == 0 {
			cctx, cancel := context.WithCancel(ctx)
			opts := []etcd.OpOption{etcd.WithRev(resp.Header.Revision), etcd.WithPrefix()}
			wch := client.Watch(cctx, l.LeaderKey, opts...)
			for kv == nil {
				wr, ok := <-wch
				if !ok || wr.Err() != nil {
					cancel()
					return
				}
				for _, ev := range wr.Events {
					if ev.Type == mvccpb.PUT {
						hdr, kv = &wr.Header, ev.Kv
						hdr.Revision = kv.ModRevision
						break
					}
				}
			}
			cancel()
		} else {
			hdr, kv = resp.Header, resp.Kvs[0]
		}

		select {
		case retc <- string(kv.Value):
		case <-ctx.Done():
			return
		}

		cctx, cancel := context.WithCancel(ctx)
		wch := client.Watch(cctx, string(kv.Key), etcd.WithRev(hdr.Revision+1))
		keyDeleted := false
		for !keyDeleted {
			wr, ok := <-wch
			if !ok {
				cancel()
				return
			}
			for _, ev := range wr.Events {
				if ev.Type == mvccpb.DELETE {
					keyDeleted = true
					retc <- ""
					break
				}
				select {
				case retc <- string(ev.Kv.Value):
				case <-cctx.Done():
					cancel()
					return
				}
			}
		}
		cancel()
	}
}
