package anonymize

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var (
	debug         bool
	anonymizedMap string
	timeout       time.Duration
	fields        []string
	// DefaultLocalAddr is the default local IP address an Attacker uses.
	defaultLocalAddr = net.IPAddr{IP: net.IPv4zero}
	// DefaultConnections is the default amount of max open idle connections per
	// target host.
	defaultConnections = 10000
)

func init() {
	RootCmd.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "Timeout to be used in connections to ES.")
	RootCmd.Flags().BoolVar(&debug, "debug", false, "Whether to dump requests and responses.")
	RootCmd.Flags().StringSliceVar(&fields, "fields", []string{}, "Name of the fields in the source document that must be anonymized. Only accept numbers and strings.")
	RootCmd.Flags().StringVar(&anonymizedMap, "anonymized_map_path", "", "Path to the dictionary of anonymized fields.")
}

// RootCmd is the root of the anonymize command.
var RootCmd = &cobra.Command{
	Use:   "anonymize_index",
	Short: "Outputs all items in the index applying some form of anonymization.",
	Long:  "Outputs all items in the index applying some form of anonymization. The anonymous result can be bulk-inserted into another elasticsearch index.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("please set the url argument")
		}
		url := args[0]
		fmt.Println(url)

		// Match all query:
		// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-all-query.html
		client := &http.Client{
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					LocalAddr: &net.TCPAddr{IP: defaultLocalAddr.IP, Zone: defaultLocalAddr.Zone},
					KeepAlive: 3 * timeout,
					Timeout:   timeout,
				}).Dial,
				ResponseHeaderTimeout: timeout,
				MaxIdleConnsPerHost:   defaultConnections,
			},
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		req, err := http.NewRequest("GET", url, strings.NewReader(`"query": {"match_all": {}}`))
		if err != nil {
			// If we can not create request, interrupt processing.
			return err
		}
		req.WithContext(ctx)

		if debug {
			dReq, _ := httputil.DumpRequest(req, true)
			fmt.Println(string(dReq))
		}

		resp, err := client.Do(req)
		if err != nil {
			// If request can not be sent due to connection reasons, interrupt processing.
			return err
		}
		defer resp.Body.Close()

		if debug {
			dResp, _ := httputil.DumpResponse(resp, true)
			fmt.Println(string(dResp))
		}
		if resp.StatusCode != http.StatusOK {
			dReq, _ := httputil.DumpRequest(req, true)
			dResp, _ := httputil.DumpResponse(resp, true)
			return fmt.Errorf("invalid status code. want:200 got:%d.req:%s, resp:%s\n", resp.StatusCode, string(dReq), string(dResp))

		}
		searchResp := struct {
			Hits struct {
				Hits []struct {
					Source map[string]interface{} `json:"_source"`
				} `json:"hits"`
			} `json:"hits"`
		}{}
		if err := json.NewDecoder(resp.Body).Decode(&searchResp); err != nil {
			// If response has changed its format, interrupt processing.
			dReq, _ := httputil.DumpRequest(req, true)
			dResp, _ := httputil.DumpResponse(resp, true)
			return fmt.Errorf("error parsing response %q. req:%s, resp:%s\n", err, string(dReq), string(dResp))
		}
		// NOTE: This is needed to guarantee we have a stable formatting and the regular expression works properly.
		b, err := json.MarshalIndent(searchResp, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))
		return nil
	},
}
