// hitcounter command uses the search pattern (passed in via STDIN) to count the number of hits of
// each term inside the passed-in dictionary. The result is an ordered list of pairs term:count
// (descending on the number of hits), which is written to STDOUT.
package hitcounter

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

const (
	rdictVar = "$RDICT"
)

var (
	numClients int
	debug      bool
	dict       string
	timeout    time.Duration
	// DefaultLocalAddr is the default local IP address an Attacker uses.
	defaultLocalAddr = net.IPAddr{IP: net.IPv4zero}
	// DefaultConnections is the default amount of max open idle connections per
	// target host.
	defaultConnections = 10000
)

func init() {
	RootCmd.Flags().StringVar(&dict, "dictionary_file", "", "Newline delimited strings dictionary file.")
	RootCmd.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "Timeout to be used in connections to ES.")
	RootCmd.Flags().BoolVar(&debug, "debug", false, "Dump requests and responses.")
	RootCmd.Flags().IntVarP(&numClients, "num_clients", "c", 10, "Number of active clients making requests.")
}

type Hit struct {
	Term  string
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

		// TODO(danielfireman): Refactor client creation code between here and replay packages.
		clients := make(chan *http.Client, numClients)
		for i := 0; i < numClients; i++ {
			clients <- &http.Client{
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
		}
		errChan := make(chan error)
		var hits HitsByCount
		dictF, err := os.Open(dict)
		if err != nil {
			return err
		}
		defer dictF.Close()
		count := 0
		scanner := bufio.NewScanner(dictF)
		wg := sync.WaitGroup{}
		for ; scanner.Scan(); count++ {
			wg.Add(1)
			go func(term string, count int) {
				defer wg.Done()

				client := <-clients
				defer func() {
					clients <- client
				}()

				query := strings.Replace(query, rdictVar, term, 1)

				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()

				req, err := http.NewRequest("GET", url, strings.NewReader(query))
				if err != nil {
					// If we can not create request, interrupt processing.
					errChan <- err
					return
				}
				req.WithContext(ctx)

				dReq, _ := httputil.DumpRequest(req, true)
				if debug {
					fmt.Printf("Processing term: %s\n", term)
					fmt.Println(string(dReq))
				}

				resp, err := client.Do(req)
				if err != nil {
					// If request can not be sent due to connection reasons, interrupt processing.
					errChan <- err
					return
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
					fmt.Fprintf(os.Stderr, "invalid status code. want:200 got:%d. term:%s lineno:%d req:%s, resp:%s\n", code, term, count+1, string(dReq), string(dResp))
					return
				}
				searchResp := struct {
					Hits struct {
						Total int64 `json:"total"`
					} `json:"hits"`
				}{}
				if err := json.NewDecoder(resp.Body).Decode(&searchResp); err != nil {
					// If response has changed its format, interrupt processing.
					dReq, _ := httputil.DumpRequest(req, true)
					dResp, _ := httputil.DumpResponse(resp, true)
					errChan <- fmt.Errorf("error parsing response %q. term:%s lineno:%d req:%s, resp:%s\n", err, term, count+1, string(dReq), string(dResp))
					return
				}
				hits = append(hits, Hit{Term: term, Count: searchResp.Hits.Total})
				return
			}(scanner.Text(), count)
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		go func() {
			wg.Wait()
			close(errChan)
		}()
		for err := range errChan {
			return err
		}
		// Ranging over the sorted results and making a smaller struct.
		sort.Sort(hits)
		// Writing everything to stdout.
		writer := bufio.NewWriter(os.Stdout)
		defer writer.Flush()

		out, err := json.MarshalIndent(hits, "", " ")
		if err != nil {
			return err
		}
		fmt.Fprint(writer, string(out))
		fmt.Fprintf(os.Stderr, "sent %d requests.", count)
		return nil
	},
}
