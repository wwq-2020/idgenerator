package etcd

import (
	"log"

	"github.com/wwq1988/idgenerator/pkg/conf"
	etcd "go.etcd.io/etcd/clientv3"
)

// MustInitEtcd MustInitEtcd
func MustInitEtcd(conf *conf.KV) *etcd.Client {
	etcd, err := etcd.New(etcd.Config{Endpoints: conf.Endpoints})
	if err != nil {
		log.Fatalf("failed new etcd,err:%#v", err)
	}
	return etcd
}
