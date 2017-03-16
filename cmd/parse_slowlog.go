package cmd

import (
	"github.com/spf13/cobra"

	"bufio"
	"encoding/json"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	timeLayout      = "2006-01-02 15:04:05.999"
	hostField       = "host"
	timestampField  = "ts"
	indexField      = "index"
	typesField      = "types"
	searchTypeField = "search_type"
	sourceField     = "source"
	numFields       = 6
)

var parseSlowlogCmd = &cobra.Command{
	Use:   "parseslowlog",
	Short: "Outputs a replayable version of the slowlog passed in via stdin.",
	Long:  "Outputs a replayable version of the slowlog passed in via stdin.",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Regular expression setup.
		// The solution is based on regexp's named matches. For each entry, we build a map of
		// of fields and values. This map is encoded as json and (buffered) written to stdout.
		re, err := regexp.Compile(`\[(?P<ts>[^]]+)\].?\[.*\].?\[.*\].?\[(?P<host>[^]]+)\].?\[(?P<index>[^]]+)\].?\[.*\].*types\[(?P<types>[^]]+)\].*search_type\[(?P<search_type>[^]]+)\].*source\[(?P<source>[^]]+)\]`)
		if err != nil {
			return err
		}
		subExpNames := re.SubexpNames()

		// Writer and encoding configuration.
		writer := bufio.NewWriter(os.Stdout)
		defer writer.Flush()
		enc := json.NewEncoder(writer)

		var previousTime time.Time
		fields := make(map[string]string, numFields)
		scanner := bufio.NewScanner(os.Stdin)
		for count := 0; scanner.Scan(); count++ {
			// Building a map using named matches.
			matches := re.FindAllStringSubmatch(scanner.Text(), -1)[0]
			for i, n := range matches {
				// Removing the first match, which is the whole line.
				if i > 0 {
					fields[subExpNames[i]] = n
				}
			}
			// Adjusting time layout.
			fields[timestampField] = strings.Replace(fields[timestampField], ",", ".", 1)

			// Overwriting stuff.
			if addr != "" {
				fields[hostField] = addr
			}
			if index != "" {
				fields[indexField] = index
			}

			// Making timestamp relative to the previous one. Simulate inter-arrival time can be as easy
			// as a time.Sleep and trigger a goroutine.
			if count == 0 {
				previousTime, err = time.Parse(timeLayout, fields[timestampField])
				if err != nil {
					return err
				}
				fields[timestampField] = "0"
			} else {
				currTime, err := time.Parse(timeLayout, fields[timestampField])
				if err != nil {
					return err
				}
				fields[timestampField] = strconv.FormatInt(currTime.Sub(previousTime).Nanoseconds(), 10)
				previousTime = currTime
			}
			if err := enc.Encode(fields); err != nil {
				return err
			}
			// Cleaning up fields.
			fields[timestampField] = ""
			fields[hostField] = ""
			fields[indexField] = ""
			fields[typesField] = ""
			fields[searchTypeField] = ""
			fields[sourceField] = ""

		}
		if err := scanner.Err(); err != nil {
			return err
		}
		return nil
	},
}
