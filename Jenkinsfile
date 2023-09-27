pipeline {
    agent any
    
    environment {
        GITHUB_TOKEN = credentials('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')
    }
    
    stages {
        stage('Merge Pull Request') {
            steps {
                script {
                    def prNumber = 6 // Replace with the PR number you want to merge
                    def apiUrl = "https://api.github.com/repos/michTalebzadeh/rhes76_DSBQ/pulls/${prNumber}/merge}/merge"
                    
                    def response = httpRequest(
                        url: apiUrl,
                        httpMode: 'POST',
                        authentication: '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',
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


