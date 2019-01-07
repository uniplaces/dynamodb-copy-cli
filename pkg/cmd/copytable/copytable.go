package copytable

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/uniplaces/dynamodbcopy"
)

const (
	cmdName          = "copy-table"
	shortDescription = "Copies dynamoDB records from a source to a target table"
)

const (
	srcTableKey   = "source-table"
	trgTableKey   = "target-table"
	srcProfileKey = "source-profile"
	trgProfileKey = "target-profile"
	readUnitsKey  = "read-units"
	writeUnitsKey = "write-units"
)

func New(config *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use:   fmt.Sprintf("%s <source-table> <target-table>", cmdName),
		Short: shortDescription,
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			config.SetDefault("source-table", args[0])
			config.SetDefault("target-table", args[1])

			service, err := wireDependencies(config)
			if err != nil {
				log.Fatalf("%s error: %s", cmdName, err)
			}

			if err := RunCopyTable(service); err != nil {
				log.Fatalf("%s error: %s", cmdName, err)
			}
		},
	}

	if err := SetAndBindFlags(cmd.Flags(), config); err != nil {
		panic(err)
	}

	return cmd
}

func wireDependencies(config *viper.Viper) (dynamodbcopy.Copier, error) {
	copyConfig, err := dynamodbcopy.NewConfig(*config)
	if err != nil {
		return nil, err
	}

	srcTableService := dynamodbcopy.NewDynamoDBService(
		config.GetString(srcTableKey),
		dynamodbcopy.NewDynamoDBAPI(config.GetString(srcProfileKey)),
		dynamodbcopy.RandomSleeper,
	)
	trgTableService := dynamodbcopy.NewDynamoDBService(
		config.GetString(trgTableKey),
		dynamodbcopy.NewDynamoDBAPI(config.GetString(trgProfileKey)),
		dynamodbcopy.RandomSleeper,
	)

	return dynamodbcopy.NewDynamoDBCopy(copyConfig, srcTableService, trgTableService)
}

func SetAndBindFlags(flagSet *pflag.FlagSet, config *viper.Viper) error {
	flagSet.StringP(srcProfileKey, "s", "", "Set the profile to use for the source table")
	flagSet.StringP(trgProfileKey, "t", "", "Set the profile to use for the target table")
	flagSet.IntP(readUnitsKey, "r", 0, "Set the read provisioned capacity for the source table")
	flagSet.IntP(writeUnitsKey, "w", 0, "Set the write provisioned capacity for the target table")

	return config.BindPFlags(flagSet)
}

func RunCopyTable(service dynamodbcopy.Copier) error {
	initialProvisioning, err := service.FetchProvisioning()
	if err != nil {
		return err
	}

	if _, err = service.UpdateProvisioning(initialProvisioning); err != nil {
		return err
	}

	if err := service.Copy(); err != nil {
		return err
	}

	if _, err := service.UpdateProvisioning(initialProvisioning); err != nil {
		return err
	}

	return nil
}
