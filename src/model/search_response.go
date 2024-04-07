package model

type SearchResponse struct {
  Took     int    `json:"took"`
  TimeOut  bool   `json:"time_out"`
  Hits     Hits   `json:"hits"`
  ScrollId string `json:"_scroll_id"`
}

type Hits struct {
  Total    int     `json:"total"`
  MaxScore float32 `json:"max_score"`
  Hits     []Hit   `json:"hits"`
}
type Hit struct {
  Index  string      `json:"_index"`
  Type   string      `json:"_type"`
  Id     string      `json:"_id"`
  Source interface{} `json:"_source"`
}
