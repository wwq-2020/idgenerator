package partition

import (
	"context"
	"sync"

	"github.com/wwq1988/idgenerator/pkg/idfetcher"
	"github.com/wwq1988/idgenerator/pkg/idgetter"
	"github.com/wwq1988/idgenerator/pkg/idmanager"
	"github.com/wwq1988/idgenerator/pkg/leadershipmanager"
)

// Factory Factory
type Factory func() idgetter.IDGetter

type partion struct {
	m map[string]idmanager.IDManager
	sync.Mutex
	idFetcherFactory  idfetcher.Factory
	idManagerFactory  idmanager.Factory
	leadershipManager leadershipmanager.LeadershipManager
}

// CreateFactory CreateFactory
func CreateFactory(idfetcherFactory idfetcher.Factory,
	idmanagerFactory idmanager.Factory,
	leadershipManager leadershipmanager.LeadershipManager) Factory {
	return func() idgetter.IDGetter {
		return New(idfetcherFactory,
			idmanagerFactory,
			leadershipManager)
	}
}

// New New
func New(idFetcherFactory idfetcher.Factory,
	idManagerFactory idmanager.Factory,
	leadershipManager leadershipmanager.LeadershipManager) idgetter.IDGetter {
	return &partion{
		idFetcherFactory:  idFetcherFactory,
		idManagerFactory:  idManagerFactory,
		leadershipManager: leadershipManager,
		m:                 make(map[string]idmanager.IDManager),
	}
}

func (p *partion) GetID(ctx context.Context, biz string) (int64, error) {
	idmanager, err := p.getIDManager(biz)
	if err != nil {
		return 0, err
	}
	id, err := idmanager.GetID()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (p *partion) getIDManager(biz string) (idmanager.IDManager, error) {
	p.Lock()
	defer p.Unlock()

	idManager, exist := p.m[biz]
	if exist {
		return idManager, nil
	}
	leadership, err := p.leadershipManager.Get(biz)
	if err != nil {
		return nil, err
	}
	idFetchFunc := p.idFetcherFactory(leadership, biz)
	idManager = p.idManagerFactory(idFetchFunc, leadership)
	p.m[biz] = idManager
	return idManager, nil
}
