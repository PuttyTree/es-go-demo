package entity

type Config struct {
	Env              string    `json:"env"`
	Disk             string    `json:"disk"`
	ParentDir        string    `json:"parentDir"`
	Year             string    `json:"year"`
	BatchSize        int       `json:"batchSize"`
	ThreadNum        int       `json:"threadNum"`
	OneFileThreadNum int       `json:"oneFileThreadNum"`
	LocalIp          string    `json:"localIp"`
	EsrpHost         string    `json:"esrpHost"`
	ZhaohuUsers      string    `json:"zhaohuUsers"`
	Reader           Parameter `json:"reader"`
	Writer           Parameter `json:"writer"`
}

type Parameter struct {
	Cluster   string `json:"cluster"`
	Endpoint  string `json:"endpoint"`
	AccessId  string `json:"accessId"`
	AccessKey string `json:"accessKey"`
	Index     string `json:"index"`
	Type      string `json:"type"`
}
