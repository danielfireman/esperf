package loadspec

import (
	"math/rand"
	"time"

	"github.com/spf13/cobra"
)

const (
	timeLayout = "2006-01-02 15:04:05.999"
)

var (
	randGen    = rand.New(rand.NewSource(time.Now().UnixNano()))
)

var RootCmd = &cobra.Command{
	Use:   "loadspec",
	Short: "Generates loadspecs for esperf",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.HelpFunc()(cmd, args)
	},
}

func init() {
	RootCmd.AddCommand(parseSlowlogCmd)
	RootCmd.AddCommand(genLoadspec)
}
