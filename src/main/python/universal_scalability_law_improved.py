import numpy as np
import matplotlib.pyplot as plt

def calculate_usl(N, sigma, kappa):
    """
    Calculate speedup using Universal Scalability Law
    
    Args:
        N: Number of nodes/replicas
        sigma: Contention penalty (coordination overhead)
        kappa: Coherency penalty (consensus communication cost)
    
    Returns:
        Performance multiplier relative to single node
    """
    return N / (1 + sigma * (N - 1) + kappa * N * (N - 1))

def find_optimal_cluster_size(sigma, kappa):
    """
    Find the optimal cluster size where performance peaks
    
    Args:
        sigma: Contention penalty (coordination overhead)
        kappa: Coherency penalty (consensus communication cost)
    
    Returns:
        N_opt: Optimal number of nodes
    """
    if kappa == 0:
        return float('inf') if sigma == 0 else 1/np.sqrt(sigma)
    return np.sqrt((1 - sigma)/(2 * kappa))

def plot_distributed_system_performance():
    """
    Plot performance scaling for distributed systems with practical labels
    """
    cluster_sizes = np.linspace(1, 32, 100)  # Test up to 32 nodes
    
    # Create main figure
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Distributed System Performance Scaling Analysis', fontsize=16)
    
    # Plot 1: Impact of Coordination Overhead (σ)
    coordination_overheads = [0, 0.1, 0.2, 0.3]
    coordination_labels = ['No overhead', 'Low overhead (10%)', 'Medium overhead (20%)', 'High overhead (30%)']
    
    for overhead, label in zip(coordination_overheads, coordination_labels):
        performance_gain = [calculate_usl(n, overhead, 0) for n in cluster_sizes]
        ax1.plot(cluster_sizes, performance_gain, label=label, linewidth=2)
    
    ax1.plot(cluster_sizes, cluster_sizes, 'k--', label='Perfect Linear Scaling', alpha=0.7)
    ax1.set_xlabel('Cluster Size (Number of Nodes)', fontsize=12)
    ax1.set_ylabel('Performance Multiplier\n(vs Single Node)', fontsize=12)
    ax1.set_title('Impact of Coordination Overhead\n(No Consensus Cost)', fontsize=13)
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_xlim(1, 32)
    
    # Plot 2: Impact of Consensus Communication Cost (κ)
    consensus_costs = [0, 0.01, 0.02, 0.05]
    consensus_labels = ['No consensus cost', 'Low cost (1%)', 'Medium cost (2%)', 'High cost (5%)']
    
    for cost, label in zip(consensus_costs, consensus_labels):
        performance_gain = [calculate_usl(n, 0.1, cost) for n in cluster_sizes]
        ax2.plot(cluster_sizes, performance_gain, label=label, linewidth=2)
    
    ax2.plot(cluster_sizes, cluster_sizes, 'k--', label='Perfect Linear Scaling', alpha=0.7)
    ax2.set_xlabel('Cluster Size (Number of Nodes)', fontsize=12)
    ax2.set_ylabel('Performance Multiplier\n(vs Single Node)', fontsize=12)
    ax2.set_title('Impact of Consensus Communication Cost\n(10% Coordination Overhead)', fontsize=13)
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_xlim(1, 32)
    
    # Plot 3: Resource Efficiency
    for overhead, label in zip(coordination_overheads, coordination_labels):
        efficiency = [calculate_usl(n, overhead, 0)/n for n in cluster_sizes]
        ax3.plot(cluster_sizes, efficiency, label=label, linewidth=2)
    
    ax3.set_xlabel('Cluster Size (Number of Nodes)', fontsize=12)
    ax3.set_ylabel('Resource Efficiency\n(Performance per Node)', fontsize=12)
    ax3.set_title('Resource Efficiency vs Cluster Size\n(Higher is Better)', fontsize=13)
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    ax3.set_xlim(1, 32)
    ax3.set_ylim(0, 1.1)
    
    # Plot 4: Real-world distributed system scenarios
    scenarios = [
        ('Ideal System (No Overhead)', 0, 0, 'green'),
        ('Well-Designed System', 0.05, 0.005, 'blue'),
        ('Legacy System (High Coordination)', 0.3, 0.005, 'orange'),
        ('Complex Consensus (High Communication)', 0.05, 0.05, 'red'),
        ('Poorly Designed System', 0.3, 0.05, 'purple')
    ]
    
    for name, sigma, kappa, color in scenarios:
        performance_gain = [calculate_usl(n, sigma, kappa) for n in cluster_sizes]
        ax4.plot(cluster_sizes, performance_gain, label=name, linewidth=2, color=color)
        
        # Find and plot optimal point
        N_opt = find_optimal_cluster_size(sigma, kappa)
        if 1 <= N_opt <= 32:  # Only plot if within our range
            opt_performance = calculate_usl(N_opt, sigma, kappa)
            ax4.plot(N_opt, opt_performance, 'o', color=color, markersize=8)
            ax4.annotate(f'Optimal: {N_opt:.1f} nodes',
                        (N_opt, opt_performance),
                        xytext=(10, 10),
                        textcoords='offset points',
                        fontsize=9,
                        bbox=dict(boxstyle='round,pad=0.3', facecolor=color, alpha=0.3))
    
    ax4.plot(cluster_sizes, cluster_sizes, 'k--', label='Perfect Linear Scaling', alpha=0.7)
    ax4.set_xlabel('Cluster Size (Number of Nodes)', fontsize=12)
    ax4.set_ylabel('Performance Multiplier\n(vs Single Node)', fontsize=12)
    ax4.set_title('Real-World Distributed System Scenarios', fontsize=13)
    ax4.legend(fontsize=9)
    ax4.grid(True, alpha=0.3)
    ax4.set_xlim(1, 32)
    
    plt.tight_layout()
    plt.show()

def plot_practical_performance_metrics():
    """
    Plot throughput and response time with practical business metrics
    """
    cluster_sizes = np.linspace(1, 32, 100)
    
    # Create figure
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('Business Impact: Throughput and Response Time Scaling', fontsize=16)
    
    scenarios = [
        ('Ideal System', 0, 0, 'green'),
        ('Well-Optimized', 0.05, 0.005, 'blue'),
        ('Typical System', 0.1, 0.01, 'orange'),
        ('Legacy/Complex', 0.3, 0.05, 'red')
    ]
    
    base_throughput = 1000  # requests/second with 1 node
    base_response_time = 10  # milliseconds with 1 node
    
    for name, sigma, kappa, color in scenarios:
        # Calculate performance scaling
        performance_multiplier = [calculate_usl(n, sigma, kappa) for n in cluster_sizes]
        
        # Convert to business metrics
        throughput_rps = [multiplier * base_throughput for multiplier in performance_multiplier]
        response_time_ms = [base_response_time / multiplier for multiplier in performance_multiplier]
        
        # Plot throughput
        ax1.plot(cluster_sizes, throughput_rps, label=name, linewidth=2, color=color)
        
        # Plot response time
        ax2.plot(cluster_sizes, response_time_ms, label=name, linewidth=2, color=color)
    
    # Throughput plot
    ax1.set_xlabel('Cluster Size (Number of Nodes)', fontsize=12)
    ax1.set_ylabel('System Throughput\n(Requests per Second)', fontsize=12)
    ax1.set_title('Throughput Scaling\n(Higher is Better)', fontsize=13)
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_xlim(1, 32)
    
    # Response time plot
    ax2.set_xlabel('Cluster Size (Number of Nodes)', fontsize=12)
    ax2.set_ylabel('Average Response Time\n(Milliseconds)', fontsize=12)
    ax2.set_title('Response Time Scaling\n(Lower is Better)', fontsize=13)
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_xlim(1, 32)
    ax2.set_yscale('log')  # Log scale for response time
    
    plt.tight_layout()
    plt.show()

def plot_consensus_algorithm_comparison():
    """
    Plot comparison of different consensus algorithms with practical context
    """
    cluster_sizes = np.linspace(3, 21, 100)  # Consensus typically needs odd numbers
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('Consensus Algorithm Performance Comparison', fontsize=16)
    
    # Different consensus algorithms with their typical overhead characteristics
    algorithms = [
        ('Single Leader (Raft)', 0.05, 0.001, 'blue'),
        ('Multi-Paxos (Optimized)', 0.1, 0.005, 'green'),
        ('Basic Paxos', 0.2, 0.02, 'orange'),
        ('Byzantine Fault Tolerance', 0.4, 0.08, 'red'),
        ('Complex Multi-Round Protocol', 0.6, 0.15, 'purple')
    ]
    
    base_consensus_rate = 500  # consensus decisions per second with 3 nodes
    
    for name, sigma, kappa, color in algorithms:
        # Calculate performance scaling
        performance_multiplier = [calculate_usl(n, sigma, kappa) for n in cluster_sizes]
        consensus_rate = [multiplier * base_consensus_rate for multiplier in performance_multiplier]
        
        # Calculate consensus latency (inverse relationship)
        consensus_latency = [1000 / rate for rate in consensus_rate]  # ms per decision
        
        # Plot consensus rate
        ax1.plot(cluster_sizes, consensus_rate, label=name, linewidth=2, color=color)
        
        # Plot consensus latency
        ax2.plot(cluster_sizes, consensus_latency, label=name, linewidth=2, color=color)
        
        # Mark optimal points
        N_opt = find_optimal_cluster_size(sigma, kappa)
        if 3 <= N_opt <= 21:
            opt_rate = calculate_usl(N_opt, sigma, kappa) * base_consensus_rate
            ax1.plot(N_opt, opt_rate, 'o', color=color, markersize=6)
    
    # Consensus rate plot
    ax1.set_xlabel('Cluster Size (Number of Replicas)', fontsize=12)
    ax1.set_ylabel('Consensus Decisions\nper Second', fontsize=12)
    ax1.set_title('Consensus Throughput\n(Higher is Better)', fontsize=13)
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_xlim(3, 21)
    
    # Consensus latency plot
    ax2.set_xlabel('Cluster Size (Number of Replicas)', fontsize=12)
    ax2.set_ylabel('Average Consensus Time\n(Milliseconds per Decision)', fontsize=12)
    ax2.set_title('Consensus Latency\n(Lower is Better)', fontsize=13)
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_xlim(3, 21)
    ax2.set_yscale('log')
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    print("Generating Improved Distributed System Performance Visualizations...")
    print("Using practical, business-focused metrics and labels...")
    
    # Plot distributed system performance with practical labels
    plot_distributed_system_performance()
    
    # Plot business impact metrics
    plot_practical_performance_metrics()
    
    # Plot consensus algorithm comparison
    plot_consensus_algorithm_comparison()
    
    print("Visualizations complete! Much more intuitive labels for real-world usage.") 