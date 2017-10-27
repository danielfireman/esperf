package loadspec

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/danielfireman/esperf/anon"
	"github.com/danielfireman/esperf/loadspec"
	"github.com/spf13/cobra"
)

var (
	indexOverride []string
	maxDuration   time.Duration
	anonymizedMap string
	anonFields    []string
)

func init() {
	parseSlowlogCmd.Flags().StringSliceVar(&indexOverride, "index_override", []string{}, "Override slowlog indexes. It is a list, flag could be repeated if you would the loadtest to hit many indexes.")
	parseSlowlogCmd.Flags().DurationVar(&maxDuration, "max_duration", time.Duration(0), "Maximum duration of the generated loadspec. It could be smaller, if the slowlog comprise a smaller time frame.")
	parseSlowlogCmd.Flags().StringVar(&anonymizedMap, "anonymized_map_path", "", "Path to the dictionary of anonymized fields.")
	parseSlowlogCmd.Flags().StringSliceVar(&anonFields, "anon_fields", []string{}, "Name of the fields in the source document that must be anonymized. Only accept numbers and strings.")
}

var parseSlowlogCmd = &cobra.Command{
	Use:   "parseslowlog",
	Short: "Outputs a replayable loadspec based on the passed-in slowlog and parameters.",
	Long:  "Outputs a replayable loadspec based on the passed-in slowlog and parameters.",
	RunE: func(cmd *cobra.Command, args []string) error {
		var urlArg string
		if len(args) > 0 {
			// To keep in par with gen, we only consider the host or host:port part of the URL.
			urlArg = args[0]
			prefix := ""
			switch {
			case strings.HasPrefix(urlArg, "http://"):
				urlArg = strings.TrimPrefix(urlArg, "http://")
				prefix = "http://"
			case strings.HasPrefix(urlArg, "https://"):
				urlArg = strings.TrimPrefix(urlArg, "https://")
				prefix = "https://"
			}
			i := strings.Index(urlArg, "/")
			if i > 0 {
				urlArg = urlArg[:i]
			}
			urlArg = prefix + urlArg
		}

		var anonymizer *anon.Anonymizer
		if anonymizedMap != "" {
			anonymizer = &anon.Anonymizer{
				FMap: anon.MustReadFieldsMap(anonymizedMap),
				FRE:  anon.MustParseFieldsRE(anonFields),
			}
		}

		var entries loadspec.ByDelaySinceLastNanos
		scanner := bufio.NewScanner(os.Stdin)
		count := 0
		for scanner.Scan() {
			logEntry := decodeSlowlogEntry(scanner.Text())

			// For now, only processing queries.
			if logEntry.LogType != "index.search.slowlog.query" {
				continue
			}

			// Even though unmarshal and marshal consumes more CPU, it validates the source and sorts the fields,
			// which makes comparison much easier.
			var obj map[string]interface{}
			if err := json.Unmarshal([]byte(logEntry.Source), &obj); err != nil {
				return err
			}
			if anonymizer != nil {
				anonymizer.Anonymize(obj)
			}
			src, err := json.Marshal(obj)
			if err != nil {
				return err
			}
			logEntry.Source = string(src)

			entry := loadspec.Entry{Source: logEntry.Source}
			// Making timestamp relative to the previous one. Simulate inter-arrival time can be as easy
			// as a time.Sleep and trigger a goroutine.
			t, err := time.Parse(timeLayout, strings.Replace(logEntry.Timestamp, ",", ".", 1))
			if err != nil {
				return err
			}
			// Keeping timestamp here for post-processing bellow.
			entry.DelaySinceLastNanos = t.UnixNano()
			// Host argument is treated as full URL. This keeps consistency between here and gen command.

			host := logEntry.Host
			if urlArg != "" {
				host = urlArg
			}
			index := logEntry.Index
			if len(indexOverride) > 0 {
				index = indexOverride[count%len(indexOverride)]
			}

			// I would love to use url.URL, life is hard.
			// More on that: https://github.com/golang/go/issues/18824
			// TL;DR; We would like to use http://localhost:9200, but since go1.8 it is not allowed anymore.
			path := []string{host, index, logEntry.Types, "_search"}
			st := ""
			if logEntry.SearchType != "" {
				st = fmt.Sprintf("?search_type=%s", strings.ToLower(logEntry.SearchType))
			}
			entry.URL = fmt.Sprintf("%s%s", strings.Join(path, "/"), st)
			entries = append(entries, &entry)
			count++
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
		var elapsed, previousTimestamp, currTimestamp int64
		for i, e := range entries {
			e.ID = i
			// Adjusting from timestamp to delay since last request. That makes a lot easier to replay.
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
			elapsed += e.DelaySinceLastNanos
			if maxDuration.Nanoseconds() > 0 && elapsed >= maxDuration.Nanoseconds() {
				break
			}
		}
		fmt.Fprintf(os.Stderr, "Test duration: %v\n", time.Duration(elapsed))
		return nil
	},
}
