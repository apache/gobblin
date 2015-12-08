#!/bin/bash

script_dir=$(dirname $0)
script_name=$(basename $0)
GRADLE="$script_dir/gradlew"

function print_usage() {
    echo -e "USAGE: $0 [-remote|-local] [-hadoop1] [-noclean] [gradle_args]"
    echo
    echo -e "Publishes signed maven artifacts locally ($HOME/.m2/repository) or remotely (Sonatype)."
    echo -e "By default, Hadoop2 artifacts are generated."
    echo -e "\t-hadoop1    Generate hadoop1 artifacts"
    echo -e "\t-local      Publish to local repository"
    echo -e "\t-noclean    Don't run gradlew clean (useful if re-running)"
    echo -e "\t-remote     Publish to Sonatype repository"
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
hadoop_version=-PuseHadoop2
gradle_args=
noclean=

# Parse command line
for A in "$@" ; do
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
        -hadoop1)
            hadoop_version=
            ;;
        -h|--help|-help)
            print_usage
            exit
            ;;
        *)
            gradle_args="$gradle_args $A"
            ;;
    esac
done

if [ -z "${install_target}" ] ; then
    echo "${script_name}: missing install target"
    exit 1
fi

if [ -z "$noclean" ] ; then
    $GRADLE clean
fi
$GRADLE -PpublishToMaven -Porg.gradle.parallel=false -Porg.gradle.daemon=false -xtest $hadoop_version $gradle_args $install_target 2>&1 | tee /tmp/${script_name}.out


