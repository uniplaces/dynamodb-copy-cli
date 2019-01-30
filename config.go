package dynamodbcopy

type Config struct {
	readCapacityUnits  int64
	writeCapacityUnits int64
	readWorkers        int
	writeWorkers       int
}

// NewConfig creates a new Configuration to store the inital parameters for the copy command
func NewConfig(readUnits, writeUnits, readWorkers, writeWorkers int) Config {
	return Config{
		readCapacityUnits:  int64(readUnits),
		writeCapacityUnits: int64(writeUnits),
		readWorkers:        readWorkers,
		writeWorkers:       writeWorkers,
	}
}

// Provisioning receives the current provisioning value of a table,
// constructing a new Provisioning based on the argument value and the configuration parameters
// The new Provisioning value will return the higher read and write values between the 2
func (c Config) Provisioning(current Provisioning) Provisioning {
	src := current.Source
	if src != nil && c.readCapacityUnits > src.Read {
		src = &Capacity{Read: c.readCapacityUnits, Write: src.Write}
	}

	trg := current.Target
	if trg != nil && c.writeCapacityUnits > trg.Write {
		trg = &Capacity{Read: trg.Read, Write: c.writeCapacityUnits}
	}

	return Provisioning{Source: src, Target: trg}
}

// Workers retuns the read and write worker count
func (c Config) Workers() (int, int) {
	return c.readWorkers, c.writeWorkers
}
