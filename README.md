# dynamodbcopy
[![Build Status](https://travis-ci.org/uniplaces/dynamodbcopy.svg?branch=master)](https://travis-ci.org/uniplaces/dynamodbcopy)
[![Go Report Card](https://goreportcard.com/badge/github.com/uniplaces/dynamodbcopy)](https://goreportcard.com/report/github.com/uniplaces/dynamodbcopy)
[![codecov](https://codecov.io/gh/uniplaces/dynamodbcopy/branch/master/graph/badge.svg)](https://codecov.io/gh/uniplaces/dynamodbcopy)
[![GoDoc](https://godoc.org/github.com/uniplaces/dynamodbcopy?status.svg)](https://godoc.org/github.com/uniplaces/dynamodbcopy)
[![License](http://img.shields.io/:license-apache-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

## Development
To build and run this cmd, you'll need:
- go
- dep

### Dependencies
First fetch the vendors by running:
```
$ dep ensure
```

### Running
To run:
```
go run cmd/dynamodbcopy/main.go
```
Alternatively, you can easily build the binary by:
```
go build -o dynamodbcopy cmd/main.go
```
