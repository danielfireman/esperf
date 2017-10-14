package loadspec

import "regexp"

// These constants need to be in sync with the regular expression bellow.
const (
	logTypeField    = "log_type"
	hostField       = "host"
	timestampField  = "ts"
	indexField      = "index"
	typesField      = "types"
	searchTypeField = "search_type"
	sourceField     = "source"
	numFields       = 7
)

var matcherRE = regexp.MustCompile(`\[(?P<ts>[^]]+)\].?\[.*\].?\[(?P<log_type>[^]]+)\].?\[(?P<host>[^]]+)\].?\[(?P<index>[^]]+)\].?\[.*\].*types\[(?P<types>[^]]+)\].*search_type\[(?P<search_type>[^]]+)\].*source\[(?P<source>.*)\], extra_source`)
var subExpNames = matcherRE.SubexpNames()

type slowlogEntry struct {
	LogType    string
	Host       string
	Timestamp  string
	Index      string
	Types      string
	SearchType string
	Source     string
}

func decodeSlowlogEntry(row string) slowlogEntry {
	fields := make(map[string]string, numFields)
	matches := matcherRE.FindAllStringSubmatch(row, -1)[0]
	for i, m := range matches {
		if i > 0 { // Removing the first match, which is the whole line.
			fields[subExpNames[i]] = m
		}
	}
	return slowlogEntry{
		LogType:    fields[logTypeField],
		Host:       fields[hostField],
		Timestamp:  fields[timestampField],
		Index:      fields[indexField],
		Types:      fields[typesField],
		SearchType: fields[searchTypeField],
		Source:     fields[sourceField],
	}
}
