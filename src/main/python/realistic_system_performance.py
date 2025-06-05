import numpy as np
import matplotlib.pyplot as plt

def calculate_realistic_metrics(arrival_rate, service_rate, max_queue_size=1000):
    """
    Calculate system metrics with realistic degradation beyond saturation
    
    Args:
        arrival_rate: Incoming request rate
        service_rate: Base service rate
        max_queue_size: Maximum queue size
    """
    utilization = arrival_rate / service_rate
    
    # Base metrics
    if utilization < 1:
        # Normal operation zone
        queue_length = (utilization ** 2) / (1 - utilization)
        throughput = arrival_rate
        latency = queue_length / arrival_rate if arrival_rate > 0 else 0
        drop_rate = 0
    else:
        # Degradation zone
        degradation_factor = 1 + ((utilization - 1) ** 2)  # Quadratic degradation
        effective_service_rate = service_rate / degradation_factor
        
        throughput = min(arrival_rate, effective_service_rate)
        queue_length = max_queue_size  # Queue is full
        latency = queue_length / effective_service_rate if effective_service_rate > 0 else float('inf')
        drop_rate = arrival_rate - throughput
    
    return {
        'throughput': throughput,
        'latency': latency,
        'queue_length': min(queue_length, max_queue_size),
        'drop_rate': drop_rate,
        'utilization': min(utilization * 100, 100)
    }

def plot_realistic_performance(service_rate=100, max_queue_size=1000):
    """
    Plot realistic system performance including degradation
    """
    # Generate load levels from 10% to 200%
    load_percentages = np.linspace(10, 200, 100)
    
    # Calculate metrics
    metrics = []
    for load in load_percentages:
        arrival_rate = (load / 100) * service_rate
        metrics.append(calculate_realistic_metrics(arrival_rate, service_rate, max_queue_size))
    
    # Extract metrics for plotting
    throughputs = [m['throughput'] for m in metrics]
    latencies = [min(m['latency'] * 1000, 10000) for m in metrics]  # Convert to ms, cap at 10s
    queue_lengths = [m['queue_length'] for m in metrics]
    drop_rates = [m['drop_rate'] for m in metrics]
    
    # Create subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Realistic System Performance Under Load', fontsize=16)
    
    # Plot 1: Throughput vs Load
    ax1.plot(load_percentages, throughputs, 'b-', label='Actual Throughput')
    ax1.plot(load_percentages, [service_rate] * len(load_percentages), 'r--', 
             label='Nominal Service Rate')
    ax1.fill_between(load_percentages, 
                     throughputs, 
                     [service_rate] * len(load_percentages),
                     where=np.array(throughputs) < service_rate,
                     color='red', alpha=0.2, label='Degradation')
    ax1.set_xlabel('Load (%)')
    ax1.set_ylabel('Requests/second')
    ax1.set_title('Throughput Degradation Under Load')
    ax1.legend()
    ax1.grid(True)
    
    # Plot 2: Latency vs Load (log scale)
    ax2.semilogy(load_percentages, latencies, 'g-')
    ax2.axvline(x=100, color='r', linestyle='--', label='Saturation Point')
    ax2.set_xlabel('Load (%)')
    ax2.set_ylabel('Latency (ms) - Log Scale')
    ax2.set_title('Response Time Degradation')
    ax2.grid(True)
    ax2.legend()
    
    # Plot 3: Queue Length vs Load
    ax3.plot(load_percentages, queue_lengths, 'm-')
    ax3.axhline(y=max_queue_size, color='r', linestyle='--', label='Queue Limit')
    ax3.set_xlabel('Load (%)')
    ax3.set_ylabel('Queue Length')
    ax3.set_title('Queue Buildup')
    ax3.grid(True)
    ax3.legend()
    
    # Plot 4: Drop Rate vs Load
    ax4.plot(load_percentages, drop_rates, 'r-')
    ax4.set_xlabel('Load (%)')
    ax4.set_ylabel('Dropped Requests/second')
    ax4.set_title('Request Drop Rate')
    ax4.grid(True)
    
    plt.tight_layout()
    plt.show()

def plot_system_comparison():
    """
    Compare ideal vs realistic system behavior
    """
    load_percentages = np.linspace(10, 200, 100)
    service_rate = 100
    
    # Calculate metrics for both ideal and realistic cases
    ideal_throughput = []
    ideal_latency = []
    real_throughput = []
    real_latency = []
    
    for load in load_percentages:
        arrival_rate = (load / 100) * service_rate
        
        # Ideal case (original queuing theory)
        utilization = arrival_rate / service_rate
        if utilization < 1:
            ideal_throughput.append(arrival_rate)
            ideal_latency.append(utilization / (1 - utilization) * 1000)  # ms
        else:
            ideal_throughput.append(service_rate)
            ideal_latency.append(float('inf'))
        
        # Realistic case
        metrics = calculate_realistic_metrics(arrival_rate, service_rate)
        real_throughput.append(metrics['throughput'])
        real_latency.append(metrics['latency'] * 1000)  # ms
    
    # Create comparison plots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle('Ideal vs Realistic System Behavior', fontsize=16)
    
    # Throughput comparison
    ax1.plot(load_percentages, ideal_throughput, 'b--', label='Ideal')
    ax1.plot(load_percentages, real_throughput, 'r-', label='Realistic')
    ax1.set_xlabel('Load (%)')
    ax1.set_ylabel('Throughput (req/s)')
    ax1.set_title('Throughput Comparison')
    ax1.legend()
    ax1.grid(True)
    
    # Latency comparison (log scale)
    ax2.semilogy(load_percentages, 
                 [min(l, 10000) for l in ideal_latency], 
                 'b--', 
                 label='Ideal')
    ax2.semilogy(load_percentages, 
                 [min(l, 10000) for l in real_latency], 
                 'r-', 
                 label='Realistic')
    ax2.set_xlabel('Load (%)')
    ax2.set_ylabel('Latency (ms) - Log Scale')
    ax2.set_title('Latency Comparison')
    ax2.legend()
    ax2.grid(True)
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    print("Generating realistic system performance visualizations...")
    
    # Plot realistic performance metrics
    plot_realistic_performance()
    
    # Plot comparison between ideal and realistic behavior
    plot_system_comparison()
