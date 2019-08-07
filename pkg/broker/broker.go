package broker

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cwupup/xq/pkg/config"
	midgen "github.com/cwupup/xq/pkg/model/files/idgen"
	"github.com/cwupup/xq/pkg/model/files/meta"
	"github.com/cwupup/xq/pkg/model/idgen"
	"github.com/cwupup/xq/pkg/model/pb"
	"github.com/cwupup/xq/pkg/model/storage"
	"github.com/cwupup/xq/pkg/util"
)

type Broker struct {
	cfg        *config.Config
	dfs        storage.DFS
	meta       *pb.BrokerMeta
	elector    *ControllerElector
	hbs        *heartbeatService
	controller *controller
	connector  *controllerConnector
}

func NewBroker(cfg *config.Config) *Broker {
	return &Broker{cfg: cfg}
}

func (b *Broker) Setup() error {
	dfs, err := storage.NewDFS(b.cfg)
	if err != nil {
		return err
	}
	b.dfs = dfs

	err = b.initMeta()
	if err != nil {
		return err
	}

	elector := newControllerElector(b)
	controllerID, controllerTerm, err := elector.CurrentController()
	if err != nil {
		return err
	}
	b.elector = elector

	// there's already a controller, ask for join request
	if controllerID > 0 {
		goto JOIN
	}

ELECTION:
	// there is no controller in the cluster so far, try to begin an election.
	for retry := 0; retry < 5; retry++ {
		nowID, nowTerm, err := b.elector.TryElect(controllerID, controllerTerm)
		// win the election or someone wins
		if err == nil || err == util.ErrControllerSelectionConflict {
			controllerID = nowID
			controllerTerm = nowTerm
			goto JOIN
		}
	}
	return util.ErrControllerSelectionFatal

JOIN:

	if b.IsController() {
		b.controller = newController(controllerID, controllerTerm, b.dfs)
		b.controller.refreshBrokerMeta(b.meta)
		b.meta.State = pb.BrokerState_BOnline
		b.hbs = newHeartbeatService(b.cfg.Tick, b.cfg.TimeoutTicks, b.ID(), nil, b.dfs)
	} else {
		b.connector, err = newControllerConnector(controllerID, controllerTerm, b.dfs)
		if err != nil {
			return err
		}
		//ask the controller for a join request
		err = b.joinCluster()
		if err != nil {
			//the controller has crashed right now
			// or it crashed as the last broker before
			if err == util.ErrUnavailableBroker {
				goto ELECTION
			}
		}

		b.meta.State = pb.BrokerState_BOnline
		b.hbs = newHeartbeatService(b.cfg.Tick, b.cfg.TimeoutTicks, b.ID(), []int32{controllerID}, b.dfs)
	}

	return nil
}

func (b *Broker) ID() int32 {
	return b.meta.GetId()
}

func (b *Broker) GetMeta() *pb.BrokerMeta {
	return b.meta
}

func (b *Broker) IsController() bool {
	return b.meta.GetId() == b.controller.id
}

func (b *Broker) joinCluster() error {
	metaInfo := *b.meta
	metaInfo.State = pb.BrokerState_BOnline
	req := &pb.JoinClusterRequest{Meta: &metaInfo}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client := b.connector.getClient()
	_, err := client.JoinCluster(ctx, req)
	if err != nil {
		state := status.Convert(err)
		if state.Code() == codes.Unavailable {
			return util.ErrUnavailableBroker
		}
		return err
	}
	return nil
}

func (b *Broker) getBrokerMeta(brokerID int32) (*pb.BrokerMeta, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	txn, err := b.dfs.Begin(ctx)
	if err != nil {
		return nil, err
	}
	file := meta.GetBrokerMetaFile(txn)
	value, err := file.Read(ctx, util.EncodeInt32Asc(nil, brokerID))
	if err != nil {
		return nil, err
	}
	m := &pb.BrokerMeta{}
	err = proto.Unmarshal(value, m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (b *Broker) loadID() (int32, error) {
	var brokerID int64
	var err error

	f, err := os.OpenFile(path.Join(b.cfg.RunLog, "broker.id"), os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return -1, err
	}
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return -1, err
	}
	if len(data) > 0 {
		brokerID, err = strconv.ParseInt(string(data), 10, 64)
		if err != nil {
			return -1, err
		}
		if brokerID <= 0 {
			return -1, util.ErrInvalidBrokerID
		}
	} else {
		// this is a new broker, try to allocate an id
		brokerID, err = idgen.Next(midgen.BrokerID)
		if err != nil {
			return -1, err
		}
		f.WriteString(fmt.Sprintf("%d", brokerID))
	}

	return int32(brokerID), nil
}

func (b *Broker) initMeta() error {
	id, err := b.loadID()
	if err != nil {
		return err
	}

	b.meta = &pb.BrokerMeta{
		Id:          id,
		AdminPort:   b.cfg.AdminPort,
		ServicePort: b.cfg.ServicePort,
		HostName:    b.cfg.Hostname,
		Ip:          b.cfg.IP,
		State:       pb.BrokerState_BOffline,
	}

	return nil
}
