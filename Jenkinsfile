node {
    try {
        stage 'Build'

        deleteDir()
        
        checkout scm

        def workspace = pwd()
        def version = env.BRANCH_NAME

        if(env.BRANCH_NAME=="PNDA") {
            version = sh(returnStdout: true, script: 'git describe --abbrev=0 --tags').trim()
            checkout([$class: 'GitSCM', branches: [[name: "tags/${version}"]], extensions: [[$class: 'CleanCheckout']]])
        }

        sh """
            WORK_DIR=`pwd`
            HADOOP_VERSION=2.6.0-cdh5.4.9
            cd ~/;JAVA_BASE=`pwd`
            cd \$WORK_DIR
            # note: requires Java 8
            export JAVA_HOME="\${JAVA_BASE}/jdk1.8.0_74"
            export PATH=\$JAVA_HOME/bin:\$PATH
            ./gradlew clean build -Pversion=${version} -PuseHadoop2 -PhadoopVersion=\${HADOOP_VERSION}         
          """

        stage 'Test'
        sh '''
        '''

        stage 'Deploy' 
        build job: 'deploy-component', parameters: [[$class: 'StringParameterValue', name: 'branch', value: env.BRANCH_NAME],[$class: 'StringParameterValue', name: 'component', value: "gobblin"],[$class: 'StringParameterValue', name: 'release_path', value: "platform/releases"],[$class: 'StringParameterValue', name: 'release', value: "${workspace}/gobblin-distribution-${version}.tar.gz"]]

        emailext attachLog: true, body: "Build succeeded (see ${env.BUILD_URL})", subject: "[JENKINS] ${env.JOB_NAME} succeeded", to: "${env.EMAIL_RECIPIENTS}"

    }
    catch(error) {
        emailext attachLog: true, body: "Build failed (see ${env.BUILD_URL})", subject: "[JENKINS] ${env.JOB_NAME} failed", to: "${env.EMAIL_RECIPIENTS}"
        currentBuild.result = "FAILED"
        throw error
    }      
}
