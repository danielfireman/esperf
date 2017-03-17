package loadspec

type Entry struct {
	// By using delay since last instead of timestamp we make replay a lot easier.
	DelaySinceLastNanos int64  `json:"delay_since_last_nanos"`
	Host                string `json:"host"`
	Index               string `json:"index"`
	Types               string `json:"types"`
	SearchType          string `json:"search_type"`
	Source              string `json:"source"`
}

// ByTimestampNanos implements sort.Interface for []Entry based on
// the ByTimestampNanos field.
type ByDelaySinceLastNanos []*Entry

func (a ByDelaySinceLastNanos) Len() int {
	return len(a)
}
func (a ByDelaySinceLastNanos) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ByDelaySinceLastNanos) Less(i, j int) bool {
	return a[i].DelaySinceLastNanos < a[j].DelaySinceLastNanos
}
