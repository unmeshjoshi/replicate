import numpy as np
import matplotlib.pyplot as plt

def calculate_usl(N, sigma, kappa):
    """
    Calculate speedup using Universal Scalability Law
    
    Args:
        N: Number of processors/parallel resources
        sigma: Contention penalty (serialization)
        kappa: Coherency penalty (crosstalk)
    
    Returns:
        Speedup factor relative to single resource
    """
    return N / (1 + sigma * (N - 1) + kappa * N * (N - 1))

def find_optimal_concurrency(sigma, kappa):
    """
    Find the optimal concurrency level where speedup peaks
    
    Args:
        sigma: Contention penalty
        kappa: Coherency penalty
    
    Returns:
        N_opt: Optimal number of resources
    """
    if kappa == 0:
        return float('inf') if sigma == 0 else 1/np.sqrt(sigma)
    return np.sqrt((1 - sigma)/(2 * kappa))

def plot_usl_curves():
    """
    Plot USL curves for different contention and coherency values
    """
    N = np.linspace(1, 32, 100)  # Test up to 32 processors
    
    # Create main figure
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Universal Scalability Law Analysis', fontsize=16)
    
    # Plot 1: Impact of Contention (σ) with no coherency cost
    sigmas = [0, 0.1, 0.2, 0.3]
    for sigma in sigmas:
        speedup = [calculate_usl(n, sigma, 0) for n in N]
        ax1.plot(N, speedup, label=f'σ={sigma}')
    
    ax1.plot(N, N, 'k--', label='Linear')
    ax1.set_xlabel('Number of Processors (N)')
    ax1.set_ylabel('Speedup')
    ax1.set_title('Impact of Contention (κ=0)')
    ax1.legend()
    ax1.grid(True)
    
    # Plot 2: Impact of Coherency (κ) with fixed contention
    kappas = [0, 0.01, 0.02, 0.05]
    for kappa in kappas:
        speedup = [calculate_usl(n, 0.1, kappa) for n in N]
        ax2.plot(N, speedup, label=f'κ={kappa}')
    
    ax2.plot(N, N, 'k--', label='Linear')
    ax2.set_xlabel('Number of Processors (N)')
    ax2.set_ylabel('Speedup')
    ax2.set_title('Impact of Coherency (σ=0.1)')
    ax2.legend()
    ax2.grid(True)
    
    # Plot 3: Efficiency (Speedup/N) vs N
    for sigma in sigmas:
        efficiency = [calculate_usl(n, sigma, 0)/n for n in N]
        ax3.plot(N, efficiency, label=f'σ={sigma}')
    
    ax3.set_xlabel('Number of Processors (N)')
    ax3.set_ylabel('Efficiency (Speedup/N)')
    ax3.set_title('Efficiency vs Scale (κ=0)')
    ax3.legend()
    ax3.grid(True)
    
    # Plot 4: Real-world scenarios
    scenarios = [
        ('Low contention & coherency', 0.05, 0.005),
        ('High contention', 0.3, 0.005),
        ('High coherency', 0.05, 0.05),
        ('High both', 0.3, 0.05)
    ]
    
    for name, sigma, kappa in scenarios:
        speedup = [calculate_usl(n, sigma, kappa) for n in N]
        ax4.plot(N, speedup, label=name)
        
        # Find and plot optimal point
        N_opt = find_optimal_concurrency(sigma, kappa)
        if N_opt <= 32:  # Only plot if within our x-range
            opt_speedup = calculate_usl(N_opt, sigma, kappa)
            ax4.plot(N_opt, opt_speedup, 'ko')
            ax4.annotate(f'Optimal N={N_opt:.1f}',
                        (N_opt, opt_speedup),
                        xytext=(10, 10),
                        textcoords='offset points')
    
    ax4.plot(N, N, 'k--', label='Linear')
    ax4.set_xlabel('Number of Processors (N)')
    ax4.set_ylabel('Speedup')
    ax4.set_title('Real-world Scenarios')
    ax4.legend()
    ax4.grid(True)
    
    plt.tight_layout()
    plt.show()

def plot_throughput_latency_usl():
    """
    Plot throughput and latency predictions based on USL
    """
    N = np.linspace(1, 32, 100)
    
    # Create figure
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle('USL: Throughput and Latency Analysis', fontsize=16)
    
    scenarios = [
        ('Ideal', 0, 0),
        ('Low overhead', 0.05, 0.005),
        ('Medium overhead', 0.1, 0.01),
        ('High overhead', 0.3, 0.05)
    ]
    
    base_throughput = 1000  # requests/second with 1 processor
    
    for name, sigma, kappa in scenarios:
        # Calculate relative throughput
        speedup = [calculate_usl(n, sigma, kappa) for n in N]
        throughput = [s * base_throughput for s in speedup]
        
        # Calculate relative latency (inverse of throughput per processor)
        latency = [N[i]/throughput[i] for i in range(len(N))]
        
        # Plot throughput
        ax1.plot(N, throughput, label=name)
        
        # Plot latency
        ax2.plot(N, latency, label=name)
    
    ax1.set_xlabel('Number of Processors (N)')
    ax1.set_ylabel('Throughput (req/s)')
    ax1.set_title('Throughput vs Scale')
    ax1.legend()
    ax1.grid(True)
    
    ax2.set_xlabel('Number of Processors (N)')
    ax2.set_ylabel('Relative Latency')
    ax2.set_title('Latency vs Scale')
    ax2.legend()
    ax2.grid(True)
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    print("Generating Universal Scalability Law Visualizations...")
    
    # Plot USL curves
    plot_usl_curves()
    
    # Plot throughput and latency analysis
    plot_throughput_latency_usl()
