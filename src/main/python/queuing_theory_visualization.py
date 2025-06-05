import numpy as np
import matplotlib.pyplot as plt
from queuing_theory import calculate_system_metrics

def plot_system_metrics(service_rate=100, max_queue_size=1000):
    """
    Create visualizations of system performance metrics using matplotlib
    
    Args:
        service_rate: Rate at which system can process requests
        max_queue_size: Maximum queue size
    """
    # Generate load percentages from 10% to 150%
    load_percentages = np.linspace(10, 150, 100)
    
    # Initialize arrays for metrics
    throughputs = []
    latencies = []
    queue_lengths = []
    utilizations = []
    drop_rates = []
    
    # Calculate metrics for each load percentage
    for load in load_percentages:
        arrival_rate = (load / 100) * service_rate
        metrics = calculate_system_metrics(arrival_rate, service_rate, max_queue_size)
        
        throughputs.append(metrics['effective_throughput'])
        latencies.append(min(metrics['avg_wait_time'] * 1000, 1000))  # Cap at 1000ms for visualization
        queue_lengths.append(min(metrics['avg_queue_length'], max_queue_size))
        utilizations.append(metrics['utilization'])
        drop_rates.append(metrics['dropped_rate'])
    
    # Create subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('System Performance Metrics vs Load', fontsize=16)
    
    # Plot 1: Throughput vs Load
    ax1.plot(load_percentages, throughputs, 'b-', label='Effective Throughput')
    ax1.plot(load_percentages, [service_rate] * len(load_percentages), 'r--', label='Service Rate')
    ax1.set_xlabel('Load (%)')
    ax1.set_ylabel('Requests/second')
    ax1.set_title('Throughput vs Load')
    ax1.legend()
    ax1.grid(True)
    
    # Plot 2: Latency vs Load
    ax2.semilogy(load_percentages, latencies, 'g-')
    ax2.set_xlabel('Load (%)')
    ax2.set_ylabel('Latency (ms)')
    ax2.set_title('Latency vs Load')
    ax2.grid(True)
    
    # Plot 3: Queue Length vs Load
    ax3.plot(load_percentages, queue_lengths, 'm-')
    ax3.set_xlabel('Load (%)')
    ax3.set_ylabel('Average Queue Length')
    ax3.set_title('Queue Length vs Load')
    ax3.grid(True)
    
    # Plot 4: Drop Rate vs Load
    ax4.plot(load_percentages, drop_rates, 'r-')
    ax4.set_xlabel('Load (%)')
    ax4.set_ylabel('Dropped Requests/second')
    ax4.set_title('Drop Rate vs Load')
    ax4.grid(True)
    
    # Adjust layout and display
    plt.tight_layout()
    plt.show()

def plot_utilization_impact(service_rates=[50, 100, 200], max_queue_size=1000):
    """
    Plot comparison of different service rates
    
    Args:
        service_rates: List of service rates to compare
        max_queue_size: Maximum queue size
    """
    load_percentages = np.linspace(10, 150, 100)
    
    # Create subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle('Impact of Service Rate on System Performance', fontsize=16)
    
    for rate in service_rates:
        throughputs = []
        latencies = []
        
        for load in load_percentages:
            arrival_rate = (load / 100) * rate
            metrics = calculate_system_metrics(arrival_rate, rate, max_queue_size)
            
            throughputs.append(metrics['effective_throughput'])
            latencies.append(min(metrics['avg_wait_time'] * 1000, 1000))
        
        # Plot throughput
        ax1.plot(load_percentages, throughputs, label=f'Service Rate {rate}')
        # Plot latency
        ax2.semilogy(load_percentages, latencies, label=f'Service Rate {rate}')
    
    ax1.set_xlabel('Load (%)')
    ax1.set_ylabel('Effective Throughput (req/s)')
    ax1.set_title('Throughput vs Load')
    ax1.legend()
    ax1.grid(True)
    
    ax2.set_xlabel('Load (%)')
    ax2.set_ylabel('Latency (ms)')
    ax2.set_title('Latency vs Load')
    ax2.legend()
    ax2.grid(True)
    
    plt.tight_layout()
    plt.show()

def plot_queue_size_impact(service_rate=100, queue_sizes=[100, 500, 1000, float('inf')]):
    """
    Plot impact of different queue sizes
    
    Args:
        service_rate: Base service rate
        queue_sizes: List of queue sizes to compare
    """
    load_percentages = np.linspace(10, 150, 100)
    
    # Create subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle('Impact of Queue Size on System Performance', fontsize=16)
    
    for size in queue_sizes:
        drop_rates = []
        throughputs = []
        
        for load in load_percentages:
            arrival_rate = (load / 100) * service_rate
            metrics = calculate_system_metrics(arrival_rate, service_rate, size)
            
            drop_rates.append(metrics['dropped_rate'])
            throughputs.append(metrics['effective_throughput'])
        
        size_label = 'Infinite' if size == float('inf') else str(size)
        ax1.plot(load_percentages, drop_rates, label=f'Queue Size {size_label}')
        ax2.plot(load_percentages, throughputs, label=f'Queue Size {size_label}')
    
    ax1.set_xlabel('Load (%)')
    ax1.set_ylabel('Drop Rate (req/s)')
    ax1.set_title('Drop Rate vs Load')
    ax1.legend()
    ax1.grid(True)
    
    ax2.set_xlabel('Load (%)')
    ax2.set_ylabel('Effective Throughput (req/s)')
    ax2.set_title('Throughput vs Load')
    ax2.legend()
    ax2.grid(True)
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    print("Generating System Performance Visualizations...")
    
    # Plot basic system metrics
    plot_system_metrics(service_rate=100, max_queue_size=1000)
    
    # Plot impact of different service rates
    plot_utilization_impact(service_rates=[50, 100, 200])
    
    # Plot impact of different queue sizes
    plot_queue_size_impact()
