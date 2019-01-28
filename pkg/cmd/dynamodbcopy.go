package cmd

import (
	"github.com/spf13/cobra"
	"github.com/uniplaces/dynamodbcopy/pkg/cmd/copytable"
	"log"
	"os"
)

const cmdName = "dynamodbcopy"

func New() *cobra.Command {
	cmd := &cobra.Command{
		Use: cmdName,
	}

	cmd.AddCommand(
		copytable.New(log.New(os.Stdout, "", log.LstdFlags)),
	)

	return cmd
}
