#!/bin/bash

# Kafka Server Health Check Script
# Author: Data Engineering Team
# Description: Check connectivity to remote Kafka servers using nc (netcat)

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

# Kafka server configuration (update these with your actual servers)
KAFKA_SERVERS=(
    "kafka-server1:9094"
    "kafka-server2:9094"
    "kafka-server3:9094"
)

# If config.ini exists, try to read actual servers from it
if [[ -f "streaming_pipeline/config.ini" ]]; then
    print_status "Reading Kafka servers from config.ini..."
    
    # Extract bootstrap servers from config file
    BOOTSTRAP_SERVERS=$(grep "kafka.bootstrap.servers" streaming_pipeline/config.ini 2>/dev/null | cut -d'=' -f2 | tr -d ' ')
    
    if [[ -n "$BOOTSTRAP_SERVERS" ]]; then
        print_status "Found Kafka servers in config: $BOOTSTRAP_SERVERS"
        # Convert comma-separated list to array
        IFS=',' read -ra KAFKA_SERVERS <<< "$BOOTSTRAP_SERVERS"
    else
        print_warning "Could not find kafka.bootstrap.servers in config.ini, using default servers"
    fi
fi

# Function to check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    # Check if nc (netcat) is installed
    if ! command -v nc &> /dev/null; then
        print_error "nc (netcat) is not installed. Please install it first."
        echo "  Ubuntu/Debian: sudo apt-get install netcat-openbsd"
        echo "  CentOS/RHEL:   sudo yum install nc"
        echo "  macOS:         brew install netcat"
        exit 1
    fi
    
    print_success "Prerequisites checked!"
}

# Function to test single Kafka server
test_kafka_server() {
    local server="$1"
    local host=$(echo "$server" | cut -d':' -f1)
    local port=$(echo "$server" | cut -d':' -f2)
    
    print_status "Testing connection to $host:$port..."
    
    # Use timeout to avoid hanging
    if timeout 5 nc -vz "$host" "$port" 2>/dev/null; then
        print_success "✅ $server is reachable"
        return 0
    else
        print_error "❌ $server is NOT reachable"
        return 1
    fi
}

# Function to test all Kafka servers
test_all_servers() {
    local total_servers=${#KAFKA_SERVERS[@]}
    local healthy_servers=0
    
    print_status "Testing connectivity to $total_servers Kafka servers..."
    echo "=================================================="
    
    for server in "${KAFKA_SERVERS[@]}"; do
        if test_kafka_server "$server"; then
            ((healthy_servers++))
        fi
        echo  # Add blank line for readability
    done
    
    echo "=================================================="
    print_status "Health Check Summary:"
    echo "Total servers:   $total_servers"
    echo "Healthy servers: $healthy_servers"
    echo "Failed servers:  $((total_servers - healthy_servers))"
    
    if [[ $healthy_servers -eq $total_servers ]]; then
        print_success "🎉 All Kafka servers are healthy!"
        return 0
    elif [[ $healthy_servers -gt 0 ]]; then
        print_warning "⚠️  Some Kafka servers are unreachable, but cluster may still be functional"
        return 1
    else
        print_error "💥 All Kafka servers are unreachable!"
        return 2
    fi
}

# Function to show additional network information
show_network_info() {
    echo
    print_status "=== Additional Network Information ==="
    
    for server in "${KAFKA_SERVERS[@]}"; do
        local host=$(echo "$server" | cut -d':' -f1)
        echo "🔍 DNS lookup for $host:"
        
        # Try to resolve hostname
        if command -v nslookup &> /dev/null; then
            nslookup "$host" 2>/dev/null | grep -E "^Name:|^Address:" || echo "  DNS resolution failed"
        elif command -v host &> /dev/null; then
            host "$host" 2>/dev/null || echo "  DNS resolution failed"
        else
            echo "  DNS tools not available"
        fi
        echo
    done
}

# Function to show troubleshooting tips
show_troubleshooting_tips() {
    echo
    print_status "=== Troubleshooting Tips ==="
    echo "If servers are unreachable, check:"
    echo "1. 🌐 Network connectivity (ping the hostnames)"
    echo "2. 🔥 Firewall rules (ports 9094, 9194, 9294)"
    echo "3. 🏠 VPN connection (if servers are on private network)"
    echo "4. 📝 Server addresses in streaming_pipeline/config.ini"
    echo "5. 🔧 Kafka cluster status (contact cluster administrator)"
    echo
    echo "Manual testing commands:"
    for server in "${KAFKA_SERVERS[@]}"; do
        local host=$(echo "$server" | cut -d':' -f1)
        local port=$(echo "$server" | cut -d':' -f2)
        echo "  nc -vz $host $port"
    done
}

# Main function
main() {
    echo "🔍 Kafka Server Health Check"
    echo "============================"
    
    check_prerequisites
    
    if test_all_servers; then
        exit 0  # All healthy
    else
        show_network_info
        show_troubleshooting_tips
        exit 1  # Some or all unhealthy
    fi
}

# Handle script interruption
trap 'print_error "Health check interrupted!"; exit 1' INT

# Run main function
main "$@"
