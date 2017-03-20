package loadspec

import (
	"bufio"
	"encoding/json"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
	"net/url"

	"github.com/spf13/cobra"
	"github.com/danielfireman/esperf/loadspec"
)

var parseSlowlogCmd = &cobra.Command{
	Use:   "parseslowlog",
	Short: "Outputs a replayable loadspec based on the passed-in slowlog and parameters.",
	Long:  "Outputs a replayable loadspec based on the passed-in slowlog and parameters.",
	RunE: func(cmd *cobra.Command, args []string) error {
		// These constants need to be in sync with the regular expression bellow.
		const (
			logTypeField = "log_type"
			hostField       = "host"
			timestampField  = "ts"
			indexField      = "index"
			typesField      = "types"
			searchTypeField = "search_type"
			sourceField     = "source"
			numFields       = 6
		)

		var urlArg string
		if len(args) > 0 {
			urlArg = args[0]
		}
		// Regular expression setup.
		// The solution is based on regexp's named matches. For each entry, we build a map of
		// of fields and values. This map is encoded as json and (buffered) written to stdout.
		re, err := regexp.Compile(`\[(?P<ts>[^]]+)\].?\[.*\].?\[(?P<log_type>[^]]+)\].?\[(?P<host>[^]]+)\].?\[(?P<index>[^]]+)\].?\[.*\].*types\[(?P<types>[^]]+)\].*search_type\[(?P<search_type>[^]]+)\].*source\[(?P<source>[^]]+)\]`)
		if err != nil {
			return err
		}
		subExpNames := re.SubexpNames()

		var entries loadspec.ByDelaySinceLastNanos
		fields := make(map[string]string, numFields)
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			// Building a map using named matches.
			matches := re.FindAllStringSubmatch(scanner.Text(), -1)[0]
			for i, n := range matches {
				// Removing the first match, which is the whole line.
				if i > 0 {
					fields[subExpNames[i]] = n
				}
			}
			// For now, only processing queries.
			if fields[logTypeField] != "index.search.slowlog.query" {
				continue
			}
			entry := loadspec.Entry{Source: fields[sourceField]}
			// Making timestamp relative to the previous one. Simulate inter-arrival time can be as easy
			// as a time.Sleep and trigger a goroutine.
			t, err := time.Parse(timeLayout, strings.Replace(fields[timestampField], ",", ".", 1))
			if err != nil {
				return err
			}
			// Keeping timestamp here for post-processing bellow.
			entry.DelaySinceLastNanos = t.UnixNano()
			// Host argument is treated as full URL. This keeps consistency between here and gen command.
			var u *url.URL
			if urlArg != "" {
				u, err = url.Parse(urlArg)
				if err != nil {
					return err
				}
			} else {
				path := []string{fields[hostField], fields[indexField], fields[typesField], "_search"}
				u, err = url.Parse(strings.Join(path, "/"))
				if err != nil {
					return err
				}
				q := u.Query()
				if fields[searchTypeField] != "" {
					q.Set("search_type", strings.ToLower(fields[searchTypeField]))
				}
				u.RawQuery = q.Encode()
			}
			entry.URL = u.String()
			entries = append(entries, &entry)
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		// Slow log entries are not guaranteed to be timestamp ordered.
		sort.Sort(entries)

		// Writer and encoding configuration.
		writer := bufio.NewWriter(os.Stdout)
		defer writer.Flush()
		enc := json.NewEncoder(writer)
		var previousTimestamp, currTimestamp int64
		for i, e := range entries {
			currTimestamp = e.DelaySinceLastNanos
			if i == 0 {
				e.DelaySinceLastNanos = 0
			} else {
				e.DelaySinceLastNanos -= previousTimestamp
			}
			previousTimestamp = currTimestamp
			if err := enc.Encode(&e); err != nil {
				return err
			}
		}
		return nil
	},
}
