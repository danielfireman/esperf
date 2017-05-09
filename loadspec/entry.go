package loadspec

type Entry struct {
	// By using delay since last instead of timestamp we make replay a lot easier.
	DelaySinceLastNanos int64  `json:"delay_since_last_nanos"`
	URL                 string `json:"url"`
	Source              string `json:"source"`
    ID int `json:"id"`
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
