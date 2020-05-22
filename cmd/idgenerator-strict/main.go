package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/wwq1988/group"
	"github.com/wwq1988/idgenerator/pkg/conf"
	"github.com/wwq1988/idgenerator/pkg/etcd"
	"github.com/wwq1988/idgenerator/pkg/idfetcher"
	"github.com/wwq1988/idgenerator/pkg/idgenerator-strict/service"
	"github.com/wwq1988/idgenerator/pkg/idmanager"
	"github.com/wwq1988/idgenerator/pkg/leadership"
	"github.com/wwq1988/idgenerator/pkg/leadershipmanager"
	"github.com/wwq1988/idgenerator/pkg/partition"
	"github.com/wwq1988/idgenerator/pkg/partitionmanager"
	"github.com/wwq1988/idgenerator/pkg/server"
)

var confPath string

func init() {
	flag.StringVar(&confPath, "conf", "./conf.toml", "-conf=./conf.toml")
	flag.Parse()
}

func main() {
	conf := conf.MustParse(confPath)
	etcd := etcd.MustInitEtcd(conf.KV)
	addr := fmt.Sprintf("%s:%d", conf.Server.IP, conf.Server.Port)
	leadershipFactory := leadership.CreateFactory(addr, conf.KV, etcd)
	idManagerFactory := idmanager.CreateFactory(true)
	idFetcherFactory := idfetcher.CreateFactory(conf.KV, etcd, true)
	leadershipmanager := leadershipmanager.New(conf.KV, etcd, leadershipFactory, true)
	go leadershipmanager.ToElection()
	partitionFactory := partition.CreateFactory(idFetcherFactory, idManagerFactory, leadershipmanager)
	partitionmanager := partitionmanager.New(partitionFactory)
	svc := service.New(partitionmanager)

	httpHandler := service.HTTPPort(svc)
	server := server.New(conf.Server, httpHandler)
	group.Go(func(ctx context.Context) {
		if err := server.Start(); err != nil {
			log.Fatalf("failed to start server:%#v", err)
		}
	})
	group.CatchExitSignal(func() { server.Stop() })
	group.Wait()
}
