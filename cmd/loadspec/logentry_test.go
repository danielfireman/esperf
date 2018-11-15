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

func TestDecodeSlowlogEntry_withType(t *testing.T) {
	is := is.New(t)
	logEntry := decodeSlowlogEntry(`[2018-11-15 10:57:43,659][WARN ][index.search.slowlog.query] [] [test][0] took[23.3ms], took_millis[23], types[], stats[], search_type[QUERY_THEN_FETCH], total_shards[5], source[{"query":{"match":{"test":"test"}}}], extra_source[]`)
	is.Equal(logEntry.Timestamp, "2018-11-15 10:57:43,659")
	is.Equal(logEntry.LogType, "index.search.slowlog.query")
	is.Equal(logEntry.Host, "")
	is.Equal(logEntry.Index, "test")
	is.Equal(logEntry.Types, "")
	is.Equal(logEntry.Source, `{"query":{"match":{"test":"test"}}}`)
	is.Equal(logEntry.SearchType, "QUERY_THEN_FETCH")
}
