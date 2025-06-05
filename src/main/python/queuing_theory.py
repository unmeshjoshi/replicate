import numpy as np
import math

def calculate_system_metrics(arrival_rate, service_rate, max_queue_size=None):
    """
    Calculate system metrics using queuing theory and Little's Law
    
    Args:
        arrival_rate: Average rate at which requests arrive (λ)
        service_rate: Rate at which system can process requests (μ)
        max_queue_size: Maximum queue size (optional)
    
    Returns:
        Dictionary containing various system metrics
    """
    utilization = arrival_rate / service_rate
    
    # If utilization >= 1, system is overloaded
    if utilization >= 1:
        return {
            'utilization': utilization * 100,
            'avg_queue_length': float('inf') if not max_queue_size else max_queue_size,
            'avg_wait_time': float('inf'),
            'effective_throughput': service_rate,
            'dropped_rate': arrival_rate - service_rate,
            'status': 'OVERLOADED'
        }
    
    # M/M/1 queue formulas
    avg_queue_length = (utilization * utilization) / (1 - utilization)
    avg_system_length = utilization / (1 - utilization)  # L in Little's Law
    
    # W in Little's Law (avg time in system)
    avg_wait_time = avg_system_length / arrival_rate
    
    # If there's a max queue size, calculate drop probability
    if max_queue_size:
        # Probability of having max_queue_size requests in system
        drop_prob = ((1 - utilization) * pow(utilization, max_queue_size)) / (1 - pow(utilization, max_queue_size + 1))
        effective_throughput = arrival_rate * (1 - drop_prob)
        dropped_rate = arrival_rate * drop_prob
    else:
        effective_throughput = arrival_rate
        dropped_rate = 0
    
    return {
        'utilization': utilization * 100,
        'avg_queue_length': avg_queue_length,
        'avg_wait_time': avg_wait_time,
        'effective_throughput': effective_throughput,
        'dropped_rate': dropped_rate,
        'status': 'STABLE'
    }

def demonstrate_system_performance(service_rate=100, max_queue_size=1000):
    """
    Display system performance metrics for different load levels
    """
    print(f"\nSystem Performance Analysis")
    print(f"Service Rate: {service_rate} requests/second")
    print(f"Queue Size: {max_queue_size}")
    print("-" * 95)
    print(f"{'Load %':<10} {'Throughput':<12} {'Latency(ms)':<12} {'Queue Len':<12} {'Dropped/s':<12} {'Utilization':<12} {'Status':<10}")
    print("-" * 95)
    
    # Test different load levels
    load_percentages = [10, 30, 50, 70, 80, 90, 95, 99, 100, 110, 120, 150]
    
    for load_percentage in load_percentages:
        arrival_rate = (load_percentage / 100) * service_rate
        metrics = calculate_system_metrics(arrival_rate, service_rate, max_queue_size)
        
        # Format latency
        latency = metrics['avg_wait_time'] * 1000 if metrics['avg_wait_time'] != float('inf') else float('inf')
        latency_str = f"{latency:.1f}" if latency != float('inf') else "inf"
        
        # Format queue length
        queue_len = metrics['avg_queue_length'] if metrics['avg_queue_length'] != float('inf') else max_queue_size
        queue_str = f"{queue_len:.1f}" if queue_len != float('inf') else str(max_queue_size)
        
        print(f"{load_percentage:<10.1f} {metrics['effective_throughput']:<12.1f} {latency_str:<12} {queue_str:<12} "
              f"{metrics['dropped_rate']:<12.1f} {metrics['utilization']:<12.1f} {metrics['status']:<10}")


def analyze_specific_load(arrival_rate, service_rate, max_queue_size=None):
    """
    Analyze system behavior for specific arrival and service rates
    """
    metrics = calculate_system_metrics(arrival_rate, service_rate, max_queue_size)
    
    print(f"\nSystem Analysis:")
    print(f"Arrival Rate: {arrival_rate:.1f} requests/second")
    print(f"Service Rate: {service_rate:.1f} requests/second")
    if max_queue_size:
        print(f"Max Queue Size: {max_queue_size}")
    print("-" * 50)
    print(f"System Utilization: {metrics['utilization']:.1f}%")
    print(f"Average Queue Length: {metrics['avg_queue_length']:.2f}")
    print(f"Average Wait Time: {metrics['avg_wait_time']*1000:.2f} ms")
    print(f"Effective Throughput: {metrics['effective_throughput']:.2f} requests/second")
    print(f"Dropped Requests: {metrics['dropped_rate']:.2f} requests/second")
    print(f"System Status: {metrics['status']}")

if __name__ == "__main__":
    print("=== Queue Theory and Little's Law Demonstration ===")
    
    # Show system performance metrics for different load levels
    demonstrate_system_performance(service_rate=100, max_queue_size=1000)
    
    print("\n=== Analyzing Specific Scenarios ===")
    # Example 1: System at 90% capacity
    print("\nScenario 1: System at 90% capacity")
    analyze_specific_load(90, 100, 1000)
    
    # Example 2: System at 110% capacity (overloaded)
    print("\nScenario 2: System at 110% capacity (overloaded)")
    analyze_specific_load(110, 100, 1000)
