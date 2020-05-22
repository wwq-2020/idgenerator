package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/wwq1988/idgenerator/pkg/conf"
)

// Server Server
type Server interface {
	Start() error
	Stop() error
}

type server struct {
	httpServer *http.Server
}

// New New
func New(conf *conf.Server, handler http.Handler) Server {
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", conf.Port),
		Handler: handler,
	}
	return &server{
		httpServer: httpServer,
	}
}

func (s *server) Start() error {
	return s.httpServer.ListenAndServe()
}

func (s *server) Stop() error {
	return s.httpServer.Shutdown(context.Background())
}
