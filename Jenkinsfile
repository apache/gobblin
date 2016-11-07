node {
    try {
        stage 'Build'

        deleteDir()
        
        checkout scm

        def workspace = pwd()
        def version = env.BRANCH_NAME
        def java_home = env.JAVA_HOME

        if(env.BRANCH_NAME=="PNDA") {
            version = sh(returnStdout: true, script: 'git describe --abbrev=0 --tags').trim()
            checkout([$class: 'GitSCM', branches: [[name: "tags/${version}"]], extensions: [[$class: 'CleanCheckout']]])
        }

        sh("JAVA_HOME=${java_home} ./build.sh ${version}")

        stage 'Deploy'
        build job: 'deploy', parameters: [[$class: 'StringParameterValue', name: 'artifacts_path', value: "${workspace}/pnda-build"]]

        emailext attachLog: true, body: "Build succeeded (see ${env.BUILD_URL})", subject: "[JENKINS] ${env.JOB_NAME} succeeded", to: "${env.EMAIL_RECIPIENTS}"

    }
    catch(error) {
        emailext attachLog: true, body: "Build failed (see ${env.BUILD_URL})", subject: "[JENKINS] ${env.JOB_NAME} failed", to: "${env.EMAIL_RECIPIENTS}"
        currentBuild.result = "FAILED"
        throw error
    }      
}
