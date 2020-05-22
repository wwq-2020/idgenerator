package service

import (
	"context"
	"sync"

	"github.com/wwq1988/idgenerator/pkg/idgenerator-strict/client"
	"github.com/wwq1988/idgenerator/pkg/idgetter"
	"github.com/wwq1988/idgenerator/pkg/leadership"
	"github.com/wwq1988/idgenerator/pkg/leadershipmanager"
	"github.com/wwq1988/idgenerator/pkg/service"
)

type svc struct {
	idGetter          idgetter.IDGetter
	leadershipManager leadershipmanager.LeadershipManager
	clientFactory     client.Factory
	sync.Mutex
	lm map[string]leadership.Leadership
	cm map[string]idgetter.IDGetter
}

// New New
func New(idGetter idgetter.IDGetter, leadershipManager leadershipmanager.LeadershipManager, clientFactory client.Factory) service.Service {
	return &svc{
		idGetter:          idGetter,
		leadershipManager: leadershipManager,
		clientFactory:     clientFactory,
		lm:                make(map[string]leadership.Leadership),
		cm:                make(map[string]idgetter.IDGetter),
	}
}

func (s *svc) NewID(ctx context.Context, req *service.NewIDRequest) (*service.NewIDResp, error) {
	idGetter := s.getIDGetter(req)
	id, err := idGetter.GetID(ctx, req.Biz)
	if err != nil {
		return nil, err
	}
	return &service.NewIDResp{ID: id}, nil
}

func (s *svc) getIDGetter(req *service.NewIDRequest) idgetter.IDGetter {
	switch req.Mode {
	case service.ModeTrend:
		return s.idGetter
	case service.ModeStrict:
		return s.getClient(req.Biz)
	default:
		return s.idGetter
	}
}

func (s *svc) getClient(biz string) idgetter.IDGetter {
	s.Lock()
	defer s.Unlock()
	cm, exist := s.cm[biz]
	if exist {
		return cm
	}
	leadership, err := s.leadershipManager.Get(biz)
	if err != nil {
		return &noLeadershipHandler{err}
	}
	client := s.clientFactory(leadership)
	s.cm[biz] = client
	return client
}

type noLeadershipHandler struct {
	err error
}

func (h noLeadershipHandler) GetID(ctx context.Context, biz string) (int64, error) {
	return 0, h.err
}
