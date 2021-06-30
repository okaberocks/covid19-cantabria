pipeline {
    agent {
        node {
            label 'principal'
        }    
    }

    // parametros de entrada
    parameters {
        string(
            name: 'FIREBASE_CREDS_PATH',
            defaultValue: "firebase-covid19",
            description: "Credenciales del proyecto"
        )
    }
    stages{
        // ejecucion de container
        stage('Pull Docker image into host, run and destroy container in a given environment'){
            environment {
                FIREBASE_CREDS_PATH = credentials("${params.FIREBASE_CREDS_PATH}")
            }
            steps{
                sh "ls"
                sh "docker build --build-arg FIREBASE_CREDS_PATH=${FIREBASE_CREDS_PATH} --no-cache -t covid19update:latest ."
            }
        }
    }
    post { 
        // failure {
        //     echo 'Sending slack notification'
        //     slackSend (
        //         color: '#FF0000',
        //         channel: 'jenkins',
        //         baseUrl: 'https://icane.slack.com/services/hooks/jenkins-ci/',
        //         message: "Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})",
        //         tokenCredentialId: 'slack-token'
        //     )
        // }
    }
}