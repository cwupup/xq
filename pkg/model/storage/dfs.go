package storage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cwupup/xq/pkg/config"
)

type DFS interface {
	LocalFile(name []byte) File
	Begin(ctx context.Context) (Transaction, error)
}

type Transaction interface {
	RemoteFile(name []byte) File
	Commit(ctx context.Context) error
	Rollback() error
}

type File interface {
	Name() []byte

	Read(ctx context.Context, key []byte) ([]byte, error)
	Write(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error

	BatchRead(ctx context.Context, keys [][]byte) ([][]byte, error)
	BatchWrite(ctx context.Context, keys [][]byte, values [][]byte) error
	BatchDelete(ctx context.Context, keys [][]byte) error
	DeleteRange(ctx context.Context, start []byte, end []byte) error

	Scan(ctx context.Context, start []byte, end []byte, limit int) (keys [][]byte, values [][]byte, err error)
	ReverseScan(ctx context.Context, start []byte, end []byte, limit int) (keys [][]byte, values [][]byte, err error)
}

func NewDFS(cfg *config.Config) (DFS, error) {
	storeCfg := cfg.Storage
	switch storeCfg.Type {
	case TIKV.String():
		tikvCfg := storeCfg.Conf.(*config.TIKV)
		return NewTikvDFS(context.TODO(), tikvCfg.Addrs, tikvCfg)
	default:
		return nil, fmt.Errorf("invalid storage type: %v", storeCfg.Type)
	}
}

type TYPE int

const (
	TIKV TYPE = iota
)

func (s TYPE) String() string {
	return [...]string{
		"tikv",
	}[s]
}

func (s *TYPE) UnmarshalText(text []byte) error {
	switch string(text) {
	case "tikv":
		*s = TIKV
	default:
		return fmt.Errorf("invalid storage type: %v", string(text))
	}
	return nil
}

func (s *TYPE) UnmarshalJSON(bytes []byte) error {
	var typ string
	err := json.Unmarshal(bytes, &typ)
	if err != nil {
		return err
	}
	switch typ {
	case TIKV.String():
		*s = TIKV
	default:
		return fmt.Errorf("invalid storage type: %v", typ)
	}
	return nil
}

func (s TYPE) MarshalJSON() ([]byte, error) {
	var typ string
	switch s {
	case TIKV:
		typ = TIKV.String()
	default:
		return nil, fmt.Errorf("invalid storage type: %v", s)
	}
	return json.Marshal(typ)
}
