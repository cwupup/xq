package idgen

import (
	"github.com/cwupup/xq/pkg/model/files"
	"github.com/cwupup/xq/pkg/model/storage"
	"github.com/cwupup/xq/pkg/util"
)

type FileType int32

const (
	ClusterID FileType = iota
	BrokerID
	TopicID
	ConsumerID
)

// GetIDGenFile returns a file in path:{clusterId}_{idGenCategory}_{fileType}
func GetIDGenFile(fileType FileType, txn storage.Transaction) storage.File {
	name := make([]byte, files.PrefixLen+4)
	pos := files.FillPrefix(name, files.IDGen)
	copy(name[pos:], util.EncodeInt32Asc(nil, int32(fileType)))
	return txn.RemoteFile(name)
}
