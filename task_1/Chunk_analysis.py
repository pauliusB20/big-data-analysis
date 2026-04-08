import matplotlib.pyplot as plt

# Data points provided
chunk_sizes = [100, 1000, 10000, 50000, 100000]
execution_times = [390, 358, 370, 386, 386]

# Creating the plot
plt.plot(chunk_sizes, execution_times, marker='o', linestyle='-', color='b')

# Using a log scale for the x-axis to handle the range effectively
plt.xscale('log')

# Labels and Title
plt.title('Execution Time vs. Chunk Size (6 Cores)')
plt.xlabel('Chunk Size (Log Scale)')
plt.ylabel('Execution Time (seconds)')

# Formatting
plt.grid(True, which="both", ls="-", alpha=0.5)

# Annotating data points for clarity
for i, txt in enumerate(execution_times):
    plt.annotate(f"{txt}s", (chunk_sizes[i], execution_times[i]),
                 textcoords="offset points", xytext=(0,10), ha='center')

# Saving the figure
plt.savefig('chunk_size_performance.png')