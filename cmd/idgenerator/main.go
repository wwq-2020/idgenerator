package main

import (
	"context"
	"flag"
	"log"

	"github.com/wwq1988/group"
	"github.com/wwq1988/idgenerator/pkg/common"
	"github.com/wwq1988/idgenerator/pkg/conf"
	"github.com/wwq1988/idgenerator/pkg/etcd"
	"github.com/wwq1988/idgenerator/pkg/idfetcher"
	"github.com/wwq1988/idgenerator/pkg/idgenerator-strict/client"
	"github.com/wwq1988/idgenerator/pkg/idgenerator/service"
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
	idFetcherFactory := idfetcher.CreateFactory(conf.KV, etcd, false)
	clientFactory := client.CreateFactory(common.Client)
	leadershipFactory := leadership.CreateFactory(conf.Server.IP, conf.KV, etcd)
	leadershipManager := leadershipmanager.New(conf.KV, etcd, leadershipFactory, false)

	partitionFactory := partition.CreateFactory(idFetcherFactory, idmanager.CreateFactory(false), leadershipManager)

	partitionmanager := partitionmanager.New(partitionFactory)

	svc := service.New(partitionmanager, leadershipManager, clientFactory)
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
