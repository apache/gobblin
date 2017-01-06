#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

script_dir=$(dirname $0)
script_name=$(basename $0)
GRADLE="$script_dir/../gradlew"

function print_usage() {
    echo -e "USAGE: $0 [-remote|-local] [-noclean] [gradle_args]"
    echo
    echo -e "Publishes signed maven artifacts locally ($HOME/.m2/repository) or remotely (Sonatype)."
    echo -e "\t-local      Publish to local repository"
    echo -e "\t-noclean    Don't run gradlew clean (useful if re-running)"
    echo -e "\t-remote     Publish to Sonatype repository"
    echo -e "\t-packages   a comma-separated list of gradle paths to publish (e.g. :gobblin-api,:gobblin-core)"
    echo
    echo -e "NOTES:"
    echo -e "\t1. You need the Gobblin PGP key to sign the artifacts. If you don't have it,"
    echo -e "\t   talk to another committer to get it. You also need to add the following to"
    echo -e "\t   your $HOME/.gradle/gradle.properties file:"
    echo
    echo -e "signing.keyId=<PGP key hex id>"
    echo -e "signing.password=<PGP key password>"
    echo -e "signing.secretKeyRingFile=$HOME/.gnupg/secring.gpg"
    echo
    echo -e "\t2. To upload remotely, you'll need a Sonatype account. Visit "
    echo -e "\t   https://issues.sonatype.org/secure/Signup!default.jspa to set it up. After"
    echo -e "\t   that add to your $HOME/.gradle/gradle.properties file:"
    echo
    echo -e "ossrhUsername=<Sonatype username>"
    echo -e "ossrhPassword=<Sonatype password>"
    echo
    echo -e "\t3. Uploading remotely will upload only to the Sonatype staging directory. Follow "
    echo -e "     the steps at http://central.sonatype.org/pages/releasing-the-deployment.html to"
    echo -e "     synchronize with Maven Central."
    echo -e "\t4. Don't forget to create a gobblin_<version> tag before publishing remotely!"
    echo -e "\t5. Sometimes build with fail with an error"
    echo -e "\t   '... Failed to interpolate field: private java.lang.String ...'"
    echo -e "\t   Just re-run with -noclean"
}

if [ "$#" -eq 0 ] ; then
    print_usage
    exit
fi

install_target=
gradle_args=
noclean=
declare -a packages

# Parse command line
while [ "$#" -gt 0 ] ; do
    A="$1"
    case "$A" in
        -local)
            install_target=install
            ;;
        -noclean)
            noclean="1"
            ;;
        -remote)
            install_target=uploadArchives
            ;;
        -h|--help|-help)
            print_usage
            exit
            ;;
        -packages)
            shift
            packages=( ${1//,/ } )
            ;;
        *)
            gradle_args="$gradle_args $A"
            ;;
    esac
    shift
done

if [ -z "${install_target}" ] ; then
    echo "${script_name}: missing install target"
    exit 1
fi

declare -a package_targets
for P in "${packages[@]}" ; do
    ptarget="$P:${install_target}"
    if [ "${ptarget:0:1}" != ":" ] ; then
        ptarget=":$ptarget"
    fi
    package_targets+=( "$ptarget" )
done

if [ "${#packages[@]}" -gt 0 ] ; then
    install_target="${package_targets[@]}"
fi

if [ -z "$noclean" ] ; then
    $GRADLE clean
fi
$GRADLE -PpublishToMaven -Porg.gradle.parallel=false -Porg.gradle.daemon=false -xtest $gradle_args $install_target 2>&1 | tee /tmp/${script_name}.out
