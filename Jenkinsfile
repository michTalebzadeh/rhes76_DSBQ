pipeline {
    agent any

    environment {
        GITHUB_TOKEN  = credentials('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') // Use the ID you provided for the credential
    }

    stages {
        stage('Merge Pull Request') {
            steps {
                script {
                    def prNumber = 6 // Replace with the PR number you want to merge
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

