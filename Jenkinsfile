node {
    try {
        stage 'Build'

        def workspace = pwd()

        sh '''
            echo $PWD
            echo $BRANCH_NAME
            WORK_DIR=$PWD@script
            HADOOP_VERSION=2.6.0-cdh5.4.9
            cd ~/;JAVA_BASE=`pwd`
            cd $WORK_DIR
            # note: requires Java 8
            export JAVA_HOME="${JAVA_BASE}/jdk1.8.0_74"
            export PATH=$JAVA_HOME/bin:$PATH
            ./gradlew clean build -Pversion=${BRANCH_NAME} -PuseHadoop2 -PhadoopVersion=${HADOOP_VERSION}         
          '''

        stage 'Test'
        sh '''
        '''

        stage 'Deploy' 
        build job: 'deploy-component', parameters: [[$class: 'StringParameterValue', name: 'branch', value: env.BRANCH_NAME],[$class: 'StringParameterValue', name: 'component', value: "gobblin"],[$class: 'StringParameterValue', name: 'release_path', value: "platform/releases"],[$class: 'StringParameterValue', name: 'release', value: "${workspace}@script/gobblin-distribution-${env.BRANCH_NAME}.tar.gz"]]

        emailext attachLog: true, body: "Build succeeded (see ${env.BUILD_URL})", subject: "[JENKINS] ${env.JOB_NAME} succeeded", to: "${env.EMAIL_RECIPIENTS}"

    }
    catch(error) {
        emailext attachLog: true, body: "Build failed (see ${env.BUILD_URL})", subject: "[JENKINS] ${env.JOB_NAME} failed", to: "${env.EMAIL_RECIPIENTS}"
        currentBuild.result = "FAILED"
        throw error
    }      
}
