pipeline {
    agent any

    environment {
        GITHUB_TOKEN  = credentials('github_pat_11AEXMDKA0QcyBxNp6dDbT_vCrSVtAqhe1qEwAEGXQEiP75yfJvBQcW8YbgH5LT1rzJQNRMHJS6HTwNDr') // Use the ID you provided for the credential
    }

    stages {
        stage('Merge Pull Request') {
            steps {
                script {
                    def prNumber = 7 // Replace with the PR number you want to merge
                    def apiUrl = "https://api.github.com/repos/michTalebzadeh/rhes76_DSBQ/pulls/${prNumber}/merge"

                    def response = httpRequest(
                        url: apiUrl,
                        httpMode: 'POST',
                        authentication: GITHUB_TOKEN,
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

