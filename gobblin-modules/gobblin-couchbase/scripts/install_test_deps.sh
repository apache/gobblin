#!/bin/bash

mkdir -p mock-couchbase
git -C mock-couchbase pull || git clone https://github.com/couchbase/CouchbaseMock.git mock-couchbase
pushd mock-couchbase
mvn install
popd
