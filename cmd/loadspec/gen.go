package loadspec

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/danielfireman/esperf/loadspec"
	"github.com/spf13/cobra"
)

var (
	arrivalSpec string
	dict        string
	duration    time.Duration
)

const (
	rdictVar = "$RDICT"
)

func init() {
	genLoadspec.Flags().StringVar(&arrivalSpec, "arrival_spec", "", "Inter arrival time specification.")
	genLoadspec.Flags().StringVar(&dict, "dictionary_file", "", "Newline delimited strings dictionary file.")
	genLoadspec.Flags().DurationVar(&duration, "duration", time.Duration(0), "Test duration.")
}

// The generation of the loadspec is inspired by: https://github.com/kosho/esperf
var genLoadspec = &cobra.Command{
	Use:   "gen",
	Short: "Outputs a replayable loadspec following the passed-in parameters.",
	Long:  "Outputs a replayable loadspec following the passed-in parameters.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("please set the url argument.")
		}
		url := args[0]
		iaGen, err := newInterArrival(arrivalSpec)
		if err != nil {
			return err
		}

		buff, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		query := string(buff)

		var terms []string
		if strings.Contains(string(buff), rdictVar) {
			if dict == "" {
				return fmt.Errorf("query defintion uses $RDICT, please specify --dictionary_file.")
			}
			dictF, err := os.Open(dict)
			defer dictF.Close()
			if err != nil {
				log.Fatal(err.Error())
			}
			scanner := bufio.NewScanner(dictF)
			for scanner.Scan() {
				terms = append(terms, scanner.Text())
			}
			if err := scanner.Err(); err != nil {
				return err
			}
			if len(terms) == 0 {
				return fmt.Errorf("query defintion uses $RDICT and dictionary is empty.")
			}
		}
		// Writer and encoding configuration.
		writer := bufio.NewWriter(os.Stdout)
		defer writer.Flush()
		enc := json.NewEncoder(writer)

		finalTime := duration.Nanoseconds()
		ia := int64(0)
		entry := loadspec.Entry{}
		hasTerms := len(terms) > 0
        id := 0
		for currTime := int64(0); currTime <= finalTime; currTime += ia {
            entry.ID = id
            id++
			entry.DelaySinceLastNanos = ia
			entry.URL = url
			if hasTerms {
				entry.Source = strings.Replace(query, rdictVar, terms[randGen.Int()%len(terms)], 1)
			} else {
				entry.Source = query
			}
			if err := enc.Encode(entry); err != nil {
				return err
			}
			ia = iaGen.Next()
		}
		return nil
	},
}
