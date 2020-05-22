package service

import (
	"context"

	"github.com/wwq1988/idgenerator/pkg/idgetter"
	"github.com/wwq1988/idgenerator/pkg/service"
)

type svc struct {
	idgetter idgetter.IDGetter
}

// New New
func New(idgetter idgetter.IDGetter) service.Service {
	return &svc{
		idgetter: idgetter,
	}
}

func (s *svc) NewID(ctx context.Context, req *service.NewIDRequest) (*service.NewIDResp, error) {
	id, err := s.idgetter.GetID(ctx, req.Biz)
	if err != nil {
		return nil, err
	}
	return &service.NewIDResp{ID: id}, nil
}
