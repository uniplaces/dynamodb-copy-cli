package dynamodbcopy

import "github.com/aws/aws-sdk-go/service/dynamodb"

type TableProvisioner interface {
	NeedsUpdate() bool
	Calculate() (int64, int64)
}
type provisioner struct {
	source dynamodb.TableDescription
	target dynamodb.TableDescription
}

func NewTablesDescription(src, trg dynamodb.TableDescription) TableProvisioner {
	return provisioner{src, trg}
}

func (p provisioner) NeedsUpdate() bool {
	return false
}

func (p provisioner) Calculate() (int64, int64) {
	return 0, 0
}
