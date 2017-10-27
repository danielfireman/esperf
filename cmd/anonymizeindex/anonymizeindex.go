package anonymizeindex

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"github.com/danielfireman/esperf/anon"
	"github.com/spf13/cobra"
)

var (
	anonymizedMap     string
	anonFields        []string
	debug             bool
	from, size, total int
	ctxDuration       string
	cont              bool
)

func init() {
	RootCmd.Flags().StringSliceVar(&anonFields, "anon_fields", []string{}, "Name of the fields in the source document that must be anonymized. Only accept numbers and strings.")
	RootCmd.Flags().StringVar(&anonymizedMap, "anonymized_map_path", "", "Path to the dictionary of anonymized fields.")
	RootCmd.Flags().BoolVar(&debug, "debug", false, "Dump requests and responses.")
	RootCmd.Flags().BoolVar(&cont, "continue", false, "Reads the last scroll id from file and continues from there.")
	RootCmd.Flags().IntVar(&from, "from", 0, "The from parameter defines the per-page offset from the first result you want to fetch.")
	RootCmd.Flags().IntVar(&size, "size", 10, "The size parameter allows you to configure the maximum amount of hits to be returned per page.")
	RootCmd.Flags().IntVar(&total, "total", 0, "Total number of documents to fetch.")
	RootCmd.Flags().StringVar(&ctxDuration, "ctx_duration", "1m", "Duration string to keep the search context open.")
}

const constFileName = "last_scroll_id"

type searchResponse struct {
	ScrollID string `json:"_scroll_id"`
	Hits     struct {
		Hits []map[string]interface{} `json:"hits"`
	} `json:"hits"`
}

// RootCmd is the root of the anonymize command.
var RootCmd = &cobra.Command{
	Use:   "anonymize_index",
	Short: "Outputs all items in the index applying some form of anonymization.",
	Long:  "Outputs all items in the index applying some form of anonymization. The anonymous result can be bulk-inserted into another elasticsearch index.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("please specify the URL")
		}
		if anonymizedMap == "" {
			return fmt.Errorf("please specify anonymized_map_path")
		}

		var searchResp *searchResponse
		var err error
		var docsFetched int

		anonymizer, err := newAnonymizer(cont, anonymizedMap, anonFields)
		if err != nil {
			return err
		}

		// Reading file content and creating a new fake search response which contains only the scroll id.
		if cont {
			content, err := ioutil.ReadFile(constFileName)
			if err != nil {
				return err
			}
			searchResp = &searchResponse{ScrollID: string(content)}
		} else {
			// Clearing up previous runs.
			http.NewRequest("DELETE", fmt.Sprintf("%s/_search/scroll/_all", strings.TrimRight(args[0], "/")), nil)
			// Original request. Setting up scrolling.
			searchResp, err = makeRequest(
				fmt.Sprintf("%s/_search?scroll=%s", strings.TrimRight(args[0], "/"), ctxDuration),
				fmt.Sprintf(`{"from":%d, "size":%d,"query":{"match_all":{}}}`, from, size))
			if err != nil {
				return err
			}
			anonymizer.Anonymize(searchResp.Hits.Hits...)
			if err := printHits(searchResp); err != nil {
				return err
			}
		}

		var unusedScrollID string
		for {
			unusedScrollID = searchResp.ScrollID

			// Issuing scrolled request.
			searchResp, err = makeRequest(
				strings.TrimRight(args[0], "/")+"/_search/scroll",
				fmt.Sprintf(`{"scroll": "%s","scroll_id":"%s"}`, ctxDuration, searchResp.ScrollID),
			)
			if err != nil {
				return err
			}

			// Anonymizing and printing results.
			anonymizer.Anonymize(searchResp.Hits.Hits...)
			if err := printHits(searchResp); err != nil {
				return err
			}
			if len(searchResp.Hits.Hits) < size-from {
				break
			}
			docsFetched += len(searchResp.Hits.Hits)
			if total > 0 && docsFetched >= total {
				break
			}

			// Freeing resources from previously used scroll.
			if _, err := http.NewRequest("DELETE", fmt.Sprintf("%s/_search/scroll/%s", strings.TrimRight(args[0], "/"), unusedScrollID), nil); err != nil {
				return err
			}
		}
		// Persisting scroll ID.
		ioutil.WriteFile(constFileName, []byte(searchResp.ScrollID), 0666)
		if anonymizedMap != "" {
			anonymizer.WriteFieldsMapToFile(anonymizedMap)
		}
		return nil
	},
}

func printHits(searchResp *searchResponse) error {
	for _, hit := range searchResp.Hits.Hits {
		srcBuf, err := json.Marshal(hit)
		if err != nil {
			return err
		}
		fmt.Println(string(srcBuf))
	}
	return nil
}

func newAnonymizer(cont bool, anonMapPath string, anonFields []string) (*anon.Anonymizer, error) {
	anonMap := make(anon.FieldsMap)
	var err error
	if cont {
		anonMap, err = anon.ReadFieldsMapFromFile(anonMapPath)
		if err != nil {
			return nil, err
		}
	}
	fRE, err := anon.FieldsRegexpFromStringSlice(anonFields)
	if err != nil {
		return nil, err
	}
	return &anon.Anonymizer{FMap: anonMap, FRE: fRE}, nil
}

const maxRetries = 5

func makeRequest(u, body string) (*searchResponse, error) {
	for i := 1; ; i++ {
		req, err := http.NewRequest("GET", u, strings.NewReader(body))
		if err != nil {
			return nil, err
		}
		if debug {
			dReq, _ := httputil.DumpRequest(req, true)
			fmt.Println(string(dReq))
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			if i == maxRetries {
				return nil, err
			}
			if debug {
				fmt.Printf("Error:[%q] Retrying:[count:%d, max:%d]", err, i, maxRetries)
			}
			// Giving elasticsearch a breath.
			time.Sleep(time.Second)
			continue
		}
		if debug {
			dResp, _ := httputil.DumpResponse(resp, true)
			fmt.Println(string(dResp))
		}
		bodyBuf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		resp.Body.Close()
		var sr searchResponse
		if err := json.Unmarshal(bodyBuf, &sr); err != nil {
			return nil, fmt.Errorf("error parsing response %q", err)
		}
		return &sr, nil
	}
}
