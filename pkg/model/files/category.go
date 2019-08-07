package files

import (
	"github.com/cwupup/xq/pkg/config"
	"github.com/cwupup/xq/pkg/util"
)

type Category int32

const (
	IDGen Category = iota
	Meta
	Message
	Index
	Consumer
	Protocol
	Misc
)

const (
	PrefixLen = 10
)

// FillPrefix fills clusterID_categoryID_ in a given slice
func FillPrefix(name []byte, category Category) int {
	pos := 0
	copy(name, util.EncodeInt32Asc(nil, config.ClusterID))
	pos = pos + 4
	name[pos] = '_'
	pos++
	copy(name[pos:], util.EncodeInt32Asc(nil, int32(category)))
	pos = pos + 4
	name[pos] = '_'
	pos++
	return pos
}
