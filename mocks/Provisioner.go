// Code generated by mockery v1.0.0
package mocks

import dynamodbcopy "github.com/uniplaces/dynamodbcopy"
import mock "github.com/stretchr/testify/mock"

// Provisioner is an autogenerated mock type for the Provisioner type
type Provisioner struct {
	mock.Mock
}

// Fetch provides a mock function with given fields:
func (_m *Provisioner) Fetch() (dynamodbcopy.Provisioning, error) {
	ret := _m.Called()

	var r0 dynamodbcopy.Provisioning
	if rf, ok := ret.Get(0).(func() dynamodbcopy.Provisioning); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(dynamodbcopy.Provisioning)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Update provides a mock function with given fields: provisioning
func (_m *Provisioner) Update(provisioning dynamodbcopy.Provisioning) (dynamodbcopy.Provisioning, error) {
	ret := _m.Called(provisioning)

	var r0 dynamodbcopy.Provisioning
	if rf, ok := ret.Get(0).(func(dynamodbcopy.Provisioning) dynamodbcopy.Provisioning); ok {
		r0 = rf(provisioning)
	} else {
		r0 = ret.Get(0).(dynamodbcopy.Provisioning)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(dynamodbcopy.Provisioning) error); ok {
		r1 = rf(provisioning)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
