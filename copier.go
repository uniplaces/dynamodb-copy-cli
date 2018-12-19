package dynamodbcopy

type Copier interface {
	FetchProvisioning() (TableProvisioner, error)
	UpdateProvisioning(provisioner TableProvisioner) error
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

func (dc dynamodbCopy) FetchProvisioning() (TableProvisioner, error) {
	srcDescription, err := dc.srcTable.DescribeTable()
	if err != nil {
		return nil, err
	}

	trgDescription, err := dc.trgTable.DescribeTable()
	if err != nil {
		return nil, err
	}

	return NewTablesDescription(*srcDescription, *trgDescription), nil
}

func (dc dynamodbCopy) UpdateProvisioning(descriptions TableProvisioner) error {
	return nil
}

func (dc dynamodbCopy) Copy() error {
	return nil
}
