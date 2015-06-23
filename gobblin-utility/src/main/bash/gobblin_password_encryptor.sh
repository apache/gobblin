#!/bin/bash

script_dir=$(dirname $0)
lib_dir=${script_dir}/../lib

java -cp ${lib_dir}'/*' gobblin.util.CLIPasswordEncryptor "$@"
