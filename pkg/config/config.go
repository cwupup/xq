package config

type Config struct {
	Cluster      string  `json:"cluster"`
	ClusterID    int32   `json:"cluster_id"`
	AdminPort    int64   `json:"admin_port"`
	ServicePort  int64   `json:"service_port"`
	Hostname     string  `json:"hostname"`
	IP           string  `json:"ip"`
	Tick         int     `json:"tick"`
	TimeoutTicks int     `json:"timeout_ticks"`
	RunLog       string  `json:"run_log"`
	Storage      Storage `json:"storage"`
}

type Storage struct {
	Type string      `json:"type"`
	Conf interface{} `json:"conf"`
}

type TIKV struct {
	Addrs []string `json:"addrs"`
}

var ClusterID int32
