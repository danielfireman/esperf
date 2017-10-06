package loadspec

import "testing"
import "github.com/matryer/is"

func TestDecodeSlowlogEntry(t *testing.T) {
	is := is.New(t)
	logEntry := decodeSlowlogEntry(`[2017-07-10 13:04:23,667][TRACE][index.search.slowlog.query] [host01] [index01][11] took[2.3ms], took_millis[2], types[typesfoo], stats[], search_type[QUERY_THEN_FETCH], total_shards[126], source[{"size":50,"query":{"term":{"status":"AVAILABLE"}}}], extra_source[]`)
	is.Equal(logEntry.Timestamp, "2017-07-10 13:04:23,667")
	is.Equal(logEntry.LogType, "index.search.slowlog.query")
	is.Equal(logEntry.Host, "host01")
	is.Equal(logEntry.Index, "index01")
	is.Equal(logEntry.Types, "typesfoo")
	is.Equal(logEntry.Source, `{"size":50,"query":{"term":{"status":"AVAILABLE"}}}`)
	is.Equal(logEntry.SearchType, "QUERY_THEN_FETCH")
}
