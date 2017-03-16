package cmd

import (
	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:   "esperf",
	Short: "ElasticSearch Performance Testing tool",
	Long: `Multiplatform command line tool to load test and collect metrics from your ElasticSearch deployment.
Built with love by danielfireman and friends in Go.
Source code and documentation is available at http://github.com/danielfireman/esperf`,
	Run: func(cmd *cobra.Command, args []string) {
		// fall back on default help if no args/flags are passed.
		cmd.HelpFunc()(cmd, args)
	},
}

var (
	addr    string
	index   string
	verbose bool
)

func init() {
	RootCmd.PersistentFlags().StringVar(&addr, "addr", "http://localhost:9200", "Elastic search HTTP address")
	RootCmd.PersistentFlags().StringVar(&index, "index", "wikipediax", "Index to perform queries")
	RootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Prints out requests and responses. Good for debugging.")
	RootCmd.AddCommand(runCmd)
	RootCmd.AddCommand(parseSlowlogCmd)
}
