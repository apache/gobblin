#! /bin/bash

# Very simple script for testing UIF integration and negative test

build=0
negative_test=0

while getopts bnh opt; do
    case $opt in
        b)
            build=1
            ;;
        n)
            negative_test=1
            ;;
        h)
            echo "usage: testWorker.sh <options>
                    b build UIF jars
                    n run negative test
                    h display usage"
            exit 0
            ;;
    esac
done

export BYTEMAN_HOME=$(pwd)/test/byteman/byteman-download-2.1.4.1

if [ $build == 1 ]; then
    ligradle clean
    ligradle build
fi

if [ $negative_test == 1 ]; then
    if [ ! -e "test/byteman/byteman-download-2.1.4.1" ]; then
        cd test/byteman
        curl -OL http://downloads.jboss.org/byteman/2.1.4.1/byteman-download-2.1.4.1-full.zip
        unzip byteman-download-2.1.4.1-full.zip
        cd ../../
    fi

    byteman_param="-javaagent:${BYTEMAN_HOME}/lib/byteman.jar=script:test/byteman/negative_test.btm"
fi

classpath=$(ls build/*/libs/*jar | tr $'\012' ':' | sed 's/:$//g')

java -cp $classpath $byteman_param com.linkedin.uif.scheduler.Worker test/resource/uif.properties

