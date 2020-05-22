package leadershipmanager

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/wwq1988/idgenerator/pkg/conf"
	"github.com/wwq1988/idgenerator/pkg/leadership"
	etcd "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
)

const (
	defaultBiz string = "0"
)

// LeadershipManager LeadershipManager
type LeadershipManager interface {
	Get(string) (leadership.Leadership, error)
	ToElection()
}

// leadershipManager leadershipManager
type leadershipManager struct {
	m                   map[string]leadership.Leadership
	leadershipFactory   leadership.Factory
	etcd                *etcd.Client
	ToElectionKeyPrefix string
	sync.Mutex
	canElection bool
}

// New New
func New(conf *conf.KV,
	etcd *etcd.Client,
	leadershipFactory leadership.Factory,
	canElection bool) LeadershipManager {
	m := &leadershipManager{
		m:                   make(map[string]leadership.Leadership),
		leadershipFactory:   leadershipFactory,
		etcd:                etcd,
		ToElectionKeyPrefix: conf.ToElectionKeyPrefix,
		canElection:         canElection,
	}
	//todo biz level leadership
	leadership := leadershipFactory(defaultBiz)
	if m.canElection {
		go leadership.Election()
	}
	m.m[defaultBiz] = leadership
	return m
}

func (m *leadershipManager) Get(biz string) (leadership.Leadership, error) {
	//todo biz level leadership
	leadership, exist := m.m[defaultBiz]
	if exist {
		return leadership, nil
	}
	return nil, fmt.Errorf("has no leadership for biz:%s", biz)
}

func (m *leadershipManager) ToElection() {
	go m.watchToElection()
	m.initLeadership()
}

func (m *leadershipManager) watchToElection() {
	for {
		session, err := concurrency.NewSession(m.etcd, concurrency.WithTTL(1))
		if err != nil {
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		go m.observe(ctx)
		<-session.Done()
		session.Close()
		cancel()
	}
}

func (m *leadershipManager) initLeadership() {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		resp, err := m.etcd.Get(ctx, m.ToElectionKeyPrefix, etcd.WithPrefix())
		cancel()
		if err != nil {
			continue
		}
		for _, kv := range resp.Kvs {
			m.handlePut(string(kv.Key))
		}
		break
	}
}

func (m *leadershipManager) observe(ctx context.Context) {
	for {
		wch := m.etcd.Watch(ctx, m.ToElectionKeyPrefix, etcd.WithPrefix())
		for {
			wr, ok := <-wch
			if !ok {
				return
			}
			m.handleEvents(wr.Events)
		}
	}
}

func (m *leadershipManager) handleEvents(events []*etcd.Event) {
	for _, event := range events {
		if event.Type == etcd.EventTypeDelete {
			m.handleDelete(string(event.Kv.Key))
			continue
		}
		if event.Type == etcd.EventTypePut {
			m.handlePut(string(event.Kv.Key))
			continue
		}
	}
}

func (m *leadershipManager) handleDelete(bizStr string) {
	biz := m.parseBiz(bizStr)
	m.Lock()
	defer m.Unlock()
	leadership, exist := m.m[biz]
	if !exist {
		return
	}
	leadership.Done()
	delete(m.m, biz)
}

func (m *leadershipManager) handlePut(bizStr string) {
	biz := m.parseBiz(bizStr)
	m.Lock()
	defer m.Unlock()
	if _, exist := m.m[biz]; exist {
		return
	}
	leadership := m.leadershipFactory(biz)
	if m.canElection {
		go leadership.Election()
	}
	m.m[biz] = leadership
}

func (m *leadershipManager) parseBiz(bizStr string) string {
	return strings.TrimPrefix(bizStr, m.ToElectionKeyPrefix+"/")
}
