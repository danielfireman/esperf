package loadspec

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

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
		iaGen, err := newInterArrival(arrivalSpec)
		if err != nil {
			return err
		}

		buff, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		query := string(buff)

		if strings.Contains(string(buff), rdictVar) && dict == "" {
			return fmt.Errorf("Your query defintion uses $RDICT, which implies a dictionary file. Please specify -d <dictionary file path>.")
		}

		dictF, err := os.Open(dict)
		defer dictF.Close()
		if err != nil {
			log.Fatal(err.Error())
		}
		var terms []string
		scanner := bufio.NewScanner(dictF)
		for scanner.Scan() {
			terms = append(terms, scanner.Text())
		}

		// Writer and encoding configuration.
		writer := bufio.NewWriter(os.Stdout)
		defer writer.Flush()
		enc := json.NewEncoder(writer)

		finalTime := duration.Nanoseconds()
		ia := int64(0)
		fields := make(map[string]string, numFields)
		for currTime := int64(0); currTime <= finalTime; currTime += ia {
			fields[timestampField] = strconv.FormatInt(currTime, 10)
			fields[hostField] = addr
			fields[indexField] = index
			fields[typesField] = types
			fields[searchTypeField] = searchType
			fields[sourceField] = strings.Replace(query, rdictVar, terms[randGen.Int()%len(terms)], 1)
			if err := enc.Encode(fields); err != nil {
				return err
			}
			resetFields(fields)
			ia = iaGen.Next()
		}
		return nil
	},
}
