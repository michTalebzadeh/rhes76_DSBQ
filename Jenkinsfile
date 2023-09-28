pipeline {
    agent any
    parameters {
        string(name: 'PR_NUMBER', defaultValue: '21', description: 'Pull Request Number to Merge')
    }
    environment {
        GITHUB_TOKEN = credentials('GitHub_Credentials')
    }
    stages {
        stage('Make HTTP Request') {
            steps {
                script {
                    def apiUrl = "https://api.github.com/repos/michTalebzadeh/rhes76_DSBQ/pulls/${params.PR_NUMBER}/merge"
                    def response = sh(script: "curl -X POST $apiUrl -H 'Authorization: token ${GITHUB_TOKEN}'", returnStatus: true)
                    if (response == 0) {
                        echo "HTTP request succeeded."
                    } else {
                        error("HTTP request failed with status code: ${response}")
                    }
                }
            }
        }
        stage('Merge Pull Request') {
            steps {
                script {
                    def prNumber = params.PR_NUMBER.toInteger()
                    def apiUrl = "https://api.github.com/repos/michTalebzadeh/rhes76_DSBQ/pulls/${params.PR_NUMBER}/merge"

                    def response = httpRequest(
                        url: apiUrl,
                        httpMode: 'POST',
                        authentication: 'GitHub_Credentials',
                        ignoreSslErrors: true
                    )

                    if (response.status == 200) {
                        echo "Pull request #${prNumber} merged successfully."
                    } else {
                        echo "Failed to merge pull request #${prNumber}."
                        error("HTTP Status: ${response.status}")
                    }
                }
            }
        }
    }
}

