from scipy.stats import binom

def calculate_node_failures(total_nodes, num_failures, failure_prob):
    """
    Calculate probability of exactly N failures and N or more failures
    in a cluster of given size
    
    Args:
        total_nodes: Total number of nodes in system
        num_failures: Number of failures to calculate probability for
        failure_prob: Individual node failure probability (between 0 and 1)
    """
    # Probability of exactly num_failures
    exact_prob = binom.pmf(num_failures, total_nodes, failure_prob)
    
    # Probability of num_failures or more
    cumulative_prob = 1 - binom.cdf(num_failures - 1, total_nodes, failure_prob)
    
    # Calculate '1 in X' chance for easier interpretation
    one_in_x = int(1/cumulative_prob) if cumulative_prob > 0 else float('inf')
    
    print(f"\nFailure Analysis:")
    print(f"Total Nodes: {total_nodes}")
    print(f"Number of Failures: {num_failures}")
    print(f"Individual Node Failure Probability: {failure_prob:.1%}")
    print("-" * 50)
    # Convert to percentage with 8 decimal places for better readability
    exact_percentage = exact_prob * 100
    cumulative_percentage = cumulative_prob * 100
    
    print(f"Probability of exactly {num_failures} failures: {exact_percentage:.8f}%")
    print(f"Probability of {num_failures} or more failures: {cumulative_percentage:.8f}%")
    print(f"This is approximately a 1 in {one_in_x:,} chance")

# Example usage
if __name__ == "__main__":
    # Get input from user
    total_nodes = int(input("Enter total number of nodes: "))
    num_failures = int(input("Enter number of failures to calculate: "))
    failure_prob = float(input("Enter individual node failure probability (0-1): "))
    
    calculate_node_failures(total_nodes, num_failures, failure_prob)
