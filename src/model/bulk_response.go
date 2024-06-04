package model

type BulkResponse struct {
	Took   int       `json:"took"`
	Errors bool      `json:"errors"`
	Items  []ResItem `json:"items"`
}

type ResItem struct {
	Index ResIndex `json:"index"`
}

type ResIndex struct {
	Index       string `json:"_index"`
	Type        string `json:"_type"`
	Id          string `json:"_id"`
	Result      string `json:"result"`
	Version     int    `json:"_version"`
	SeqNo       string `json:"_seq_no"`
	PrimaryTerm int    `json:"_primary_term"`
	Status      int    `json:"status"`
}
