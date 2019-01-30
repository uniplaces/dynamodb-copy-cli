# dynamodbcopy

[![Build Status](https://travis-ci.org/uniplaces/dynamodbcopy.svg?branch=master)](https://travis-ci.org/uniplaces/dynamodbcopy)
[![Go Report Card](https://goreportcard.com/badge/github.com/uniplaces/dynamodbcopy)](https://goreportcard.com/report/github.com/uniplaces/dynamodbcopy)
[![codecov](https://codecov.io/gh/uniplaces/dynamodbcopy/branch/master/graph/badge.svg)](https://codecov.io/gh/uniplaces/dynamodbcopy)
[![GoDoc](https://godoc.org/github.com/uniplaces/dynamodbcopy?status.svg)](https://godoc.org/github.com/uniplaces/dynamodbcopy)
[![License](http://img.shields.io/:license-apache-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

Dynamodbcopy is a cli tool wrapper around the [aws-sdk](https://github.com/aws/aws-sdk-go) that allows you to copy information from one dynamodb table to another.

## Installing

Use go get to retrieve `dynamodbcopy` to add it to your GOPATH workspace, or project's Go module dependencies.

> go get github.com/uniplaces/dynamodbcopy

To update run with `-u`

> go get -u github.com/uniplaces/dynamodbcopy

### Go Modules

If you are using Go modules, your go get will default to the latest tagged version. To get a specific release version of the `dynamodbcopy` use @<tag> in your go get command.

> go get github.com/uniplaces/dynamodbcopy@v1.0.0

To get the latest repository change use @latest tag.

> go get github.com/uniplaces/dynamodbcopy@latest

## Opening Issues

If you encounter a bug start by searching the existing issues and see if others are also experiencing the issue before opening a new one. Please include the versions for `dynamodbcopy` and Go that you are using. Please also include reproduction case when appropriate.

## Contributing

Please feel free to make suggestions, create issues, fork the repository and send pull requests!

## Licence

Copyright 2018 UNIPLACES

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
