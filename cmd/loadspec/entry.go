package loadspec

type Entry struct {
	TimestampNanos int64  `json:"ts"`
	Host           string `json:"host"`
	Index          string `json:"index"`
	Types          string `json:"types"`
	SearchType     string `json:"search_type"`
	Source         string `json:"source"`
}
