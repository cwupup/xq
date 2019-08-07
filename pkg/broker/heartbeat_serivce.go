package broker

import (
	"context"
	"sync"
	"time"

	"github.com/cwupup/xq/pkg/model/files/protocol"
	"github.com/cwupup/xq/pkg/model/storage"
	"github.com/cwupup/xq/pkg/util"
)

type heartbeatService struct {
	tickTime     int
	timeoutTicks int
	selfID       int32
	selfEncodeID []byte
	targetIDs    map[int32][]byte
	stats        map[int32]*heartbeatStat
	event        chan *heartbeatEvent
	closed       chan struct{}
	file         storage.File
	wg           sync.WaitGroup
	lock         sync.RWMutex
}

type heartbeatStat struct {
	lastTimestamp int64
	tickCount     int
}

type heartbeatEvent struct {
	typ     int
	content interface{}
}

const (
	HBEventError = iota
	HBEventTimeout
)

func newHeartbeatService(tickTime int, timeoutTicks int, brokerID int32, targetBrokerIDs []int32, dfs storage.DFS) *heartbeatService {
	hb := &heartbeatService{
		tickTime:     tickTime,
		timeoutTicks: timeoutTicks,
		selfID:       brokerID,
		selfEncodeID: util.EncodeInt32Asc(nil, brokerID),
		targetIDs:    make(map[int32][]byte),
		stats:        make(map[int32]*heartbeatStat),
		event:        make(chan *heartbeatEvent, 3),
		closed:       make(chan struct{}),
		file:         protocol.GetHeartbeatFile(dfs),
	}
	for _, bid := range targetBrokerIDs {
		hb.targetIDs[bid] = util.EncodeInt32Asc(nil, bid)
	}

	hb.wg.Add(2)
	go hb.keepAlive()
	go hb.healthProbe()

	return hb
}

func (h *heartbeatService) Event() <-chan *heartbeatEvent {
	return h.event
}

func (h *heartbeatService) AddTargets(brokerIDs []int32) {
	h.lock.Lock()
	defer h.lock.Unlock()
	for _, bid := range brokerIDs {
		if h.targetIDs[bid] != nil {
			continue
		}
		h.targetIDs[bid] = util.EncodeInt32Asc(nil, bid)
	}
}

func (h *heartbeatService) RemoveTargets(brokerIDs []int32) {
	h.lock.Lock()
	defer h.lock.Unlock()
	for _, bid := range brokerIDs {
		delete(h.targetIDs, bid)
		delete(h.stats, bid)
	}
}

func (h *heartbeatService) Close() {
	close(h.closed)
	h.wg.Wait()
}

func (h *heartbeatService) keepAlive() {
	interval := time.Duration(h.tickTime) * time.Millisecond
	timer := time.NewTimer(interval)
	defer func() {
		timer.Stop()
		h.wg.Done()
	}()

	for {
		select {
		case <-timer.C:
			err := h.send()
			if err != nil {
				h.event <- &heartbeatEvent{HBEventError, err}
			}
			timer.Reset(interval)
		case <-h.closed:
			return
		}
	}
}

func (h *heartbeatService) healthProbe() {
	interval := time.Duration(h.tickTime) * time.Millisecond
	timer := time.NewTimer(interval)
	defer func() {
		timer.Stop()
		h.wg.Done()
	}()

	for {
		select {
		case <-timer.C:
			timeoutIds, err := h.probe()
			if err != nil {
				h.event <- &heartbeatEvent{HBEventError, err}
			} else if len(timeoutIds) > 0 {
				h.event <- &heartbeatEvent{HBEventTimeout, timeoutIds}
			}
			timer.Reset(interval)
		case <-h.closed:
			return
		}
	}

}

func (h *heartbeatService) send() error {
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	key := h.selfEncodeID
	value := util.EncodeInt64Asc(nil, timestamp)
	cxt, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return h.file.Write(cxt, key, value)
}

func (h *heartbeatService) probe() ([]int32, error) {
	ids, encodeIds := h.getTargetIDs()
	if len(ids) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	values, err := h.file.BatchRead(ctx, encodeIds)
	if err != nil {
		return nil, err
	}
	timestamps := make([]int64, len(values))
	for i, v := range values {
		_, timestamps[i], _ = util.DecodeInt64Asc(v)
	}

	h.lock.Lock()
	defer h.lock.Unlock()
	var timeoutIds []int32
	for i, t := range timestamps {
		id := ids[i]
		if h.stats[id] == nil {
			h.stats[id] = &heartbeatStat{}
		}
		if h.stats[id].lastTimestamp == t {
			h.stats[id].tickCount++
			if h.stats[id].tickCount == h.timeoutTicks {
				timeoutIds = append(timeoutIds, ids[i])
			}
		} else {
			h.stats[id].lastTimestamp = t
			h.stats[id].tickCount = 0
		}
	}
	return timeoutIds, nil
}

func (h *heartbeatService) getTargetIDs() ([]int32, [][]byte) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	if len(h.targetIDs) == 0 {
		return nil, nil
	}

	ids := make([]int32, 0, len(h.targetIDs))
	encodeIds := make([][]byte, 0, len(h.targetIDs))
	for bid, ebid := range h.targetIDs {
		ids = append(ids, bid)
		encodeIds = append(encodeIds, ebid)
	}
	return ids, encodeIds
}
