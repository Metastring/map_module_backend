pipeline {
    agent any

    environment {
        WORK_DIR = '/home/metastring/src/map_module_backend'
    }

    stages {

        stage('Deploy Code') {
            steps {
                sh '''
                rsync -av --delete \
                --no-owner \
                --no-group \
                --no-perms \
                --omit-dir-times \
                --exclude '.git/' \
                --exclude 'env/' \
                --exclude '__pycache__/' \
                --exclude '*.log' \
                --exclude '.pytest_cache/' \
                ${WORKSPACE}/ \
                ${WORK_DIR}/
                '''
            }
        }

        stage('Install Dependencies') {
            steps {
                dir("${WORK_DIR}") {
                    sh '''
                    . env/bin/activate
                    pip install -r requirements.txt
                    '''
                }
            }
        }

        stage('Restart Backend Service') {
            steps {
                sh '''
                sudo systemctl restart map_module_backend
                '''
            }
        }
    }

    post {
        success {
            echo '✅ Deployment Successful'
        }
        failure {
            echo '❌ Deployment Failed'
        }
    }
}
