package meta

import (
	"github.com/cwupup/xq/pkg/model/files"
	"github.com/cwupup/xq/pkg/model/storage"
	"github.com/cwupup/xq/pkg/util"
)

type FileType int32

const (
	Cluster FileType = iota
	Broker
	Topic
	Consumer
)

// GetClusterMetaFile returns a file in path:{clusterId}_{metaCategory}_{cluster}_{clusterName}
func GetClusterMetaFile(cluster string, txn storage.Transaction) storage.File {
	name := make([]byte, files.PrefixLen+5+len(cluster))
	pos := files.FillPrefix(name, files.Meta)
	copy(name[pos:], util.EncodeInt32Asc(nil, int32(Cluster)))
	pos = pos + 4
	name[pos] = '_'
	pos++
	copy(name[pos:], []byte(cluster))
	pos = pos + len(cluster)
	return txn.RemoteFile(name)
}

// GetBrokerMetaFile returns a file in path:{clusterId}_{metaCategory}_{broker}
func GetBrokerMetaFile(txn storage.Transaction) storage.File {
	name := make([]byte, files.PrefixLen+4)
	pos := files.FillPrefix(name, files.Meta)
	copy(name[pos:], util.EncodeInt32Asc(nil, int32(Broker)))
	pos = pos + 4
	return txn.RemoteFile(name)
}
