package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"

	"github.com/cwupup/xq/pkg/model/files/meta"
	"github.com/cwupup/xq/pkg/model/pb"
	"github.com/cwupup/xq/pkg/model/storage"
	"github.com/cwupup/xq/pkg/util"
)

type controllerConnector struct {
	id     int32
	term   int32
	dfs    storage.DFS
	meta   *pb.BrokerMeta
	conn   *grpc.ClientConn
	client pb.XQClient
}

func newControllerConnector(id int32, term int32, dfs storage.DFS) (*controllerConnector, error) {
	cc := &controllerConnector{id: id, term: term, dfs: dfs}
	err := cc.getControllerMeta()
	if err != nil {
		return nil, err
	}

	hostPort := fmt.Sprintf("%s:%d", cc.meta.Ip, cc.meta.ServicePort)
	cc.conn, err = grpc.DialContext(context.Background(), hostPort, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	cc.client = pb.NewXQClient(cc.conn)

	return cc, nil
}

func (cc *controllerConnector) getControllerMeta() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	txn, err := cc.dfs.Begin(ctx)
	if err != nil {
		return err
	}
	file := meta.GetBrokerMetaFile(txn)
	value, err := file.Read(ctx, util.EncodeInt32Asc(nil, cc.id))
	if err != nil {
		return err
	}
	m := &pb.BrokerMeta{}
	err = proto.Unmarshal(value, m)
	if err != nil {
		return err
	}
	cc.meta = m

	return nil
}

func (cc *controllerConnector) getClient() pb.XQClient {
	return cc.client
}

func (cc *controllerConnector) close() error {
	return cc.conn.Close()
}
