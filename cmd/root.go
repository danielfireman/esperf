package cmd

import (
	"fmt"

	"github.com/danielfireman/esperf/cmd/hitcounter"
	"github.com/danielfireman/esperf/cmd/loadspec"
	"github.com/danielfireman/esperf/cmd/replay"
	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:   "esperf",
	Short: "ElasticSearch Performance Testing tool",
	Long: `Multiplatform command line tool to load test and collect metrics from your ElasticSearch deployment.
Built with love by danielfireman and friends in Go.
Source code and documentation is available at http://github.com/danielfireman/esperf`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Root CMD")
		// fall back on default help if no args/flags are passed.
		cmd.HelpFunc()(cmd, args)
	},
}

func init() {
	RootCmd.AddCommand(replay.RootCmd)
	RootCmd.AddCommand(loadspec.RootCmd)
	RootCmd.AddCommand(hitcounter.RootCmd)
}
