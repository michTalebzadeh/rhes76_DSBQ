pipeline {
    agent any

    parameters {
        string(name: 'PR_NUMBER', defaultValue: '13', description: 'Pull Request Number to Merge')
    }

    environment {
        GITHUB_CREDENTIALS = credentials('GitHub_Credentials') // Use the ID you provided for the credential
        GITHUB_API_URL = "https://api.github.com/repos/michTalebzadeh/rhes76_DSBQ/pulls"
    }

    stages {
        stage('Merge Pull Request') {
            steps {
                script {
                    def prNumber = params.PR_NUMBER.toInteger()
                    def apiUrl = "https://api.github.com/repos/michTalebzadeh/rhes76_DSBQ/pulls/${prNumber}/merge"

                    def response = httpRequest(
                        url: apiUrl,
                        httpMode: 'POST',
                        authentication: 'GITHUB_CREDENTIALS',
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

