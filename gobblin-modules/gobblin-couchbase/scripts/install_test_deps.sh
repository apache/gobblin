#!/bin/bash

mkdir -p mock-couchbase
if [ -d mock-couchbase/.git ];
  then
    git -C mock-couchbase pull
    git -C mock-couchbase checkout b929a9f99a31a233c43bd94b6a696ad966206e02
  else
    git clone https://github.com/couchbase/CouchbaseMock.git mock-couchbase
    git -C mock-couchbase checkout b929a9f99a31a233c43bd94b6a696ad966206e02
  fi
pushd mock-couchbase
if mvn package -Dmaven.test.skip=true; then
  popd
else
  echo "Error building mock-couchbase"
  exit 1
fi
