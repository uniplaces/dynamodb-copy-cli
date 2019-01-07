package dynamodbcopy

type Copier interface {
	FetchProvisioning() (Provisioning, error)
	UpdateProvisioning(provisioning Provisioning) (Provisioning, error)
	Copy() error
}

type dynamodbCopy struct {
	config   Config
	srcTable DynamoDBService
	trgTable DynamoDBService
}

func NewDynamoDBCopy(copyConfig Config, srcTableService, trgTableService DynamoDBService) (Copier, error) {
	return dynamodbCopy{
		config:   copyConfig,
		srcTable: srcTableService,
		trgTable: trgTableService,
	}, nil
}

func (dc dynamodbCopy) FetchProvisioning() (Provisioning, error) {
	srcDescription, err := dc.srcTable.DescribeTable()
	if err != nil {
		return Provisioning{}, err
	}

	trgDescription, err := dc.trgTable.DescribeTable()
	if err != nil {
		return Provisioning{}, err
	}

	return NewProvisioning(*srcDescription, *trgDescription), nil
}

func (dc dynamodbCopy) UpdateProvisioning(provisioning Provisioning) (Provisioning, error) {
	currentProvisioning, err := dc.FetchProvisioning()
	if err != nil {
		return Provisioning{}, err
	}

	if needsProvisioningUpdate(currentProvisioning.SourceCapacity(), provisioning.SourceCapacity()) {
		if err := dc.srcTable.UpdateCapacity(provisioning.SourceCapacity()); err != nil {
			return Provisioning{}, err
		}
	}

	if needsProvisioningUpdate(currentProvisioning.TargetCapacity(), provisioning.TargetCapacity()) {
		if err := dc.trgTable.UpdateCapacity(provisioning.TargetCapacity()); err != nil {
			return Provisioning{}, err
		}
	}

	return provisioning, nil
}

func (dc dynamodbCopy) Copy() error {
	return nil
}

func needsProvisioningUpdate(c1, c2 Capacity) bool {
	return c1.Read != c2.Read || c1.Write != c2.Write
}
