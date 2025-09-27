#!/bin/bash

# Real-time Data Streaming Pipeline Deployment Script
# Author: Data Engineering Team
# Description: Deploys Spark streaming pipeline with Docker

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    # Check if Docker is installed and running
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! docker ps &> /dev/null; then
        print_error "Docker daemon is not running. Please start Docker first."
        exit 1
    fi
    
    # Check if we're in the correct directory
    if [[ ! -f "requirements.txt" ]] || [[ ! -d "streaming_pipeline" ]]; then
        print_error "Please run this script from the Le_Son_LV2_Project_02 directory."
        exit 1
    fi
    
    # Check if network exists
    if ! docker network ls | grep -q "streaming-network"; then
        print_warning "Docker network 'streaming-network' not found. Creating it..."
        docker network create streaming-network
        print_success "Created Docker network 'streaming-network'"
    fi
    
    print_success "All prerequisites checked!"
}

# Function to clean up existing containers
cleanup_containers() {
    print_status "Cleaning up existing containers..."
    
    # Stop and remove existing container
    docker container stop spark-streaming 2>/dev/null || true
    docker container rm spark-streaming 2>/dev/null || true
    
    print_success "Container cleanup completed!"
}

# Function to package Python modules
package_modules() {
    print_status "Packaging Python modules..."
    
    # Remove existing package if it exists
    rm -f streaming_modules.zip
    
    # Create the package with all necessary files
    zip -r streaming_modules.zip streaming_pipeline/*
    
    if [[ -f "streaming_modules.zip" ]]; then
        print_success "Python modules packaged successfully!"
        print_status "Package contents:"
        unzip -l streaming_modules.zip
    else
        print_error "Failed to create Python modules package!"
        exit 1
    fi
}

# Function to deploy Spark container
deploy_container() {
    print_status "Deploying Spark streaming container..."
    
    docker run -ti --name spark-streaming \
        --network=streaming-network \
        -p 4040:4040 \
        -v ./:/spark \
        -v spark_lib:/opt/bitnami/spark/.ivy2 \
        -v spark_data:/data \
        -e PYSPARK_DRIVER_PYTHON='python' \
        -e PYSPARK_PYTHON='./environment/bin/python' \
        unigap/spark:3.5 bash -c "
        echo '=== Setting up Python environment ===' &&
        python -m venv pyspark_venv &&
        source pyspark_venv/bin/activate &&
        echo '=== Installing Python dependencies ===' &&
        pip install -r /spark/requirements.txt &&
        pip install venv-pack &&
        echo '=== Packaging Python environment ===' &&
        venv-pack -o pyspark_venv.tar.gz &&
        echo '=== Starting Spark streaming pipeline ===' &&
        spark-submit \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
          --archives pyspark_venv.tar.gz#environment \
          --py-files /spark/streaming_modules.zip \
          /spark/streaming_pipeline/run_pipeline.py
        "
}

# Function to show post-deployment information
show_post_deployment_info() {
    print_success "Deployment completed successfully!"
    echo
    print_status "=== Post-Deployment Information ==="
    echo "📊 Spark UI: http://localhost:4040"
    echo "📝 Container Logs: docker logs spark-streaming"
    echo "🛑 Stop Pipeline: docker stop spark-streaming"
    echo "🗑️  Remove Container: docker rm spark-streaming"
    echo
    print_status "=== Monitoring Commands ==="
    echo "# View container logs:"
    echo "docker logs spark-streaming --tail 100 -f"
    echo
    echo "# Check container status:"
    echo "docker ps | grep spark-streaming"
    echo
    echo "# Connect to container (for debugging):"
    echo "docker exec -it spark-streaming bash"
}

# Main deployment function
main() {
    echo "🚀 Starting Spark Streaming Pipeline Deployment"
    echo "================================================"
    
    check_prerequisites
    cleanup_containers
    package_modules
    deploy_container
    show_post_deployment_info
}

# Handle script interruption
trap 'print_error "Deployment interrupted!"; exit 1' INT

# Run main function
main "$@"
