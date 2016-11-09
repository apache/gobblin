#!/bin/bash
#group is overiden to support forked repositories

function print_usage(){
  echo "maven-install.sh --version VERSION [--group GROUP]"
}

for i in "$@"
do
  case "$1" in
    --version)
      VERSION="$2"
      shift
      ;;
    --group)
      GROUP="$2"
      shift
      ;;
    --help)
      print_usage
      exit 0
      ;;
    *)
      ;;
  esac
  shift
done

if [ -z "$VERSION" ]; then
  print_usage
  exit
fi
echo VERSION=$VERSION

if [ -z "$GROUP" ]; then
 GROUP="gobblin"
fi


./gradlew install -Dorg.gradle.parallel=false -Pversion=$VERSION -Pgroup=$GROUP
