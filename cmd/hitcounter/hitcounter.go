package hitcounter

import (
	"github.com/spf13/cobra"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"log"
	"bufio"
	"time"
	"net/http"
	"net"
	"context"
	"net/http/httputil"
	"encoding/json"
	"sort"
)

const (
	rdictVar = "$RDICT"
)

var (
	debug bool
	dict string
	timeout time.Duration
	// DefaultLocalAddr is the default local IP address an Attacker uses.
	defaultLocalAddr = net.IPAddr{IP: net.IPv4zero}
	// DefaultConnections is the default amount of max open idle connections per
	// target host.
	defaultConnections = 10000
)

func init() {
	RootCmd.Flags().StringVar(&dict, "dictionary_file", "", "Newline delimited strings dictionary file.")
	RootCmd.Flags().DurationVar(&timeout, "timeout", 30 * time.Second, "Timeout to be used in connections to ES.")
	RootCmd.Flags().BoolVar(&debug, "debug", false, "Dump requests and responses.")
}

type Hit struct {
	Term string
	Count int64
}

type HitsByCount []Hit

func (a HitsByCount) Len() int {
	return len(a)
}
func (a HitsByCount) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a HitsByCount) Less(i, j int) bool {
	return a[i].Count > a[j].Count
}

var RootCmd = &cobra.Command{
	Use:   "counthits",
	Short: "Counts hits passed-in queries.",
	Long:  "Counts hits passed-in queries.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("please set the url argument.")
		}
		url := args[0]
		buff, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		query := string(buff)
		if strings.Contains(string(buff), rdictVar) && dict == "" {
			return fmt.Errorf("query defintion uses $RDICT, please set --dictionary_file.")
		}
		dictF, err := os.Open(dict)
		defer dictF.Close()
		if err != nil {
			log.Fatal(err.Error())
		}

		// TODO(danielfireman): Refactor client creation code between here and replay packages.
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
		var hits HitsByCount
		scanner := bufio.NewScanner(dictF)
		for scanner.Scan() {
			err := func() error {
				term := scanner.Text()
				query := strings.Replace(query, rdictVar, term, 1)

				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()

				req, err := http.NewRequest("GET", url, strings.NewReader(query))
				if err != nil {
					return err
				}
				req.WithContext(ctx)

				dReq, _ := httputil.DumpRequest(req, true)
				if debug {
					fmt.Println(string(dReq))
				}

				resp, err := client.Do(req)
				if err != nil {
					return err
				}
				defer resp.Body.Close()

				dResp, _ := httputil.DumpResponse(resp, true)
				if debug {
					fmt.Println(string(dResp))
				}

				code := resp.StatusCode
				if resp.StatusCode != 200 {
					dReq, _ := httputil.DumpRequest(req, true)
					dResp, _ := httputil.DumpResponse(resp, true)
					return fmt.Errorf("invalid status code. want:200 got:%d. req:%s, resp:%s", code, string(dReq), string(dResp))
				}
				searchResp := struct {
					Hits         struct {
							     Total    int64 `json:"total"`
						     } `json:"hits"`
				}{}
				if err := json.NewDecoder(resp.Body).Decode(&searchResp); err != nil {
					dReq, _ := httputil.DumpRequest(req, true)
					dResp, _ := httputil.DumpResponse(resp, true)
					return fmt.Errorf("error parsing response %q\n. req:%s, resp:%s", err,string(dReq), string(dResp))
				}
				hits = append(hits, Hit{Term:term, Count:searchResp.Hits.Total})
				return nil
			}()
			if err != nil {
				return err
			}

		}
		if err := scanner.Err(); err != nil {
			return err
		}

		sort.Sort(hits)

		// Writer and encoding configuration.
		writer := bufio.NewWriter(os.Stdout)
		defer writer.Flush()
		if err := json.NewEncoder(writer).Encode(hits); err != nil {
			return err
		}
		return nil
	},
}
