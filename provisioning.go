package dynamodbcopy

import "github.com/aws/aws-sdk-go/service/dynamodb"

type Provisioning struct {
	Source dynamodb.TableDescription
	Target dynamodb.TableDescription
}

type Capacity struct {
	Read  int64
	Write int64
}

func NewProvisioning(src, trg dynamodb.TableDescription) Provisioning {
	return Provisioning{src, trg}
}

func (p Provisioning) SourceCapacity() Capacity {
	return Capacity{
		Write: *p.Source.ProvisionedThroughput.WriteCapacityUnits,
		Read:  *p.Source.ProvisionedThroughput.ReadCapacityUnits,
	}
}

func (p Provisioning) TargetCapacity() Capacity {
	return Capacity{
		Write: *p.Target.ProvisionedThroughput.WriteCapacityUnits,
		Read:  *p.Target.ProvisionedThroughput.ReadCapacityUnits,
	}
}
