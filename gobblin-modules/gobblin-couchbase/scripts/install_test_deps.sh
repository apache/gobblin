#!/bin/bash

mkdir -p mock-couchbase
if [ -d mock-couchbase/.git ];
  then
    git -C mock-couchbase pull
  else
    git clone https://github.com/couchbase/CouchbaseMock.git mock-couchbase
  fi
pushd mock-couchbase
mvn install
popd
