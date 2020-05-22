package conf

import (
	"io/ioutil"
	"log"

	"github.com/BurntSushi/toml"
)

// Config 文件配置
type Config struct {
	Server *Server `toml:"server"`
	KV     *KV     `toml:"kv"`
}

// Server Server
type Server struct {
	Port int    `toml:"port"`
	IP   string `toml:"ip"`
}

// KV KV
type KV struct {
	Endpoints           []string `toml:"endpoints"`
	LeaderKeyFmt        string   `toml:"leader_key_fmt"`
	CurIDKeyFmt         string   `toml:"cur_id_key_fmt"`
	ElectionKeyFmt      string   `toml:"election_key_fmt"`
	LeaseTTL            int      `toml:"lease_ttl"`
	ToElectionKeyPrefix string   `toml:"to_election_key_prefix"`
}

// MustParse MustParse
func MustParse(file string) *Config {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalf("failed to read file:%s,err:%#v", file, err)
	}
	cfg := &Config{}
	if err := toml.Unmarshal(data, cfg); err != nil {
		log.Fatalf("failed to unmarshal,err:%#v", err)
	}
	return cfg
}
