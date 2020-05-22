package idmanager

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/wwq1988/idgenerator/pkg/common"
	"github.com/wwq1988/idgenerator/pkg/idfetcher"
	"github.com/wwq1988/idgenerator/pkg/leadership"
)

const (
	step = 100
)

// Factory Factory
type Factory func(idfetcher idfetcher.IDFetcher, leadership leadership.Leadership) IDManager

// IDManager IDManager
type IDManager interface {
	GetID() (int64, error)
}

type idManager struct {
	idfetcher      idfetcher.IDFetcher
	ids            []int64
	fetchLock      *sync.Mutex
	fetchCond      *sync.Cond
	idsLock        *sync.Mutex
	idsCond        *sync.Cond
	once           sync.Once
	step           int
	prevLen        int
	inited         bool
	lastFetchTime  time.Time
	shouldBeLeader bool
	leaderStatus   uint32
}

// CreateFactory CreateFactory
func CreateFactory(shouldBeLeader bool) Factory {
	return func(idfetcher idfetcher.IDFetcher, leadership leadership.Leadership) IDManager {
		return New(idfetcher, shouldBeLeader, leadership)
	}
}

// IDFetchFunc IDFetchFunc
type IDFetchFunc func(step int) []int64

// New New
func New(idfetcher idfetcher.IDFetcher, shouldBeLeader bool, leadership leadership.Leadership) IDManager {
	fetchLock := &sync.Mutex{}
	fetchCond := sync.NewCond(fetchLock)
	idsLock := &sync.Mutex{}
	idsCond := sync.NewCond(idsLock)
	m := &idManager{
		idfetcher:      idfetcher,
		ids:            make([]int64, 0, step),
		fetchLock:      fetchLock,
		fetchCond:      fetchCond,
		idsLock:        idsLock,
		idsCond:        idsCond,
		step:           step,
		prevLen:        step,
		shouldBeLeader: shouldBeLeader,
		leaderStatus:   leadership.GetLeaderStatus().ToUint32(),
	}
	leadership.OnLeaderStatusChange(m.onLeaderChange)
	m.bootstrap()
	return m
}

func (m *idManager) onLeaderChange(leaderStatus common.LeaderStatus) {
	atomic.StoreUint32(&m.leaderStatus, leaderStatus.ToUint32())
}

func (m *idManager) GetID() (int64, error) {
	m.idsLock.Lock()
	defer m.idsLock.Unlock()

retry:
	if m.shouldBeLeader && atomic.LoadUint32(&m.leaderStatus) == common.LeaderStatusNotLeader.ToUint32() {
		return 0, common.ErrNotLeader
	}
	if len(m.ids) <= m.step/2 {
		m.notifyFetch()
	}

	if len(m.ids) == 0 {
		m.idsCond.Wait()
		goto retry
	}

	id := m.ids[0]
	m.ids = m.ids[1:]
	return id, nil
}

func (m *idManager) notifyFetch() {
	m.fetchLock.Lock()
	m.fetchCond.Signal()
	m.fetchLock.Unlock()
}

func (m *idManager) bootstrap() {
	m.doFetch()
	go m.start()
}

func (m *idManager) start() {
	for {
		m.doFetchWhen()
	}
}

func (m *idManager) doFetchWhen() {
	m.fetchLock.Lock()
	m.fetchCond.Wait()
	defer m.fetchLock.Unlock()
	m.doFetch()
}

func (m *idManager) doFetch() {
	m.idsLock.Lock()
	defer m.idsLock.Unlock()

	now := time.Now()
	duration := now.Sub(m.lastFetchTime)
	if m.inited {
		m.step = int(float64(m.prevLen-len(m.ids)) / duration.Seconds())
	}
	if step <= step {
		m.step = step
	}

	m.lastFetchTime = now

	ids := m.idfetcher.NextIDRange(m.step)
	if m.shouldBeLeader && len(ids) == 0 {
		m.ids = nil
	}

	m.ids = append(m.ids, ids...)

	m.prevLen = len(m.ids)
	m.idsCond.Broadcast() // in case of deadlock
	m.inited = true
}
