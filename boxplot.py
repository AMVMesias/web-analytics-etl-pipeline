import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
import os
import time

def count_hits_per_row(file_path, sample_size=None):
    """Load CSV and count the number of hits per row"""
    try:
        print(f"Loading {file_path}...")
        start_time = time.time()
        
        # Check if file exists
        if not os.path.exists(file_path):
            print(f"Error: File {file_path} does not exist")
            return None
            
        print(f"File size: {os.path.getsize(file_path) / (1024*1024):.2f} MB")
        
        # Try different approaches to load the file
        try:
            # Use pandas with error handling for bad lines
            print("Counting hits per row...")
            
            # First count hits columns in header row
            df_header = pd.read_csv(file_path, nrows=0)
            hits_columns = [col for col in df_header.columns if str(col).startswith('hits')]
            print(f"Found {len(hits_columns)} hits columns in header")
            
            # Now process data in chunks to count non-null hits per row
            hits_counts = []
            chunk_size = 10000
            
            # Process in chunks for memory efficiency
            for chunk in pd.read_csv(
                file_path, 
                chunksize=chunk_size,
                on_bad_lines='skip',
                low_memory=False,
                encoding='latin1'
            ):
                # Filter only hits columns
                hits_chunk = chunk[[col for col in chunk.columns if str(col).startswith('hits')]]
                
                # Count non-null values per row
                row_counts = hits_chunk.count(axis=1)
                
                # Sample if needed
                if sample_size and sample_size < 1.0:
                    # Random sampling
                    mask = np.random.random(len(row_counts)) < sample_size
                    row_counts = row_counts[mask]
                
                hits_counts.extend(row_counts.tolist())
                
                if len(hits_counts) > 0 and len(hits_counts) % (chunk_size * 10) == 0:
                    print(f"Processed {len(hits_counts)} rows...")
            
            print(f"Processed {len(hits_counts)} rows total")
        
        except Exception as e:
            print(f"Error: {e}")
            print("Attempting alternative method...")
            
            # Alternative approach using csv module
            import csv
            
            hits_counts = []
            with open(file_path, 'r', encoding='latin1', errors='replace') as f:
                reader = csv.reader(f)
                headers = next(reader)
                
                # Find columns that start with 'hits'
                hits_indices = [i for i, col in enumerate(headers) if str(col).startswith('hits')]
                print(f"Found {len(hits_indices)} hits columns")
                
                if not hits_indices:
                    print(f"No columns starting with 'hits' found in {file_path}")
                    return None
                
                # Count non-null hits per row
                line_count = 0
                for row in reader:
                    line_count += 1
                    
                    # Sample if needed
                    if sample_size and sample_size < 1.0:
                        if np.random.random() > sample_size:
                            continue
                    
                    try:
                        # Count non-empty hits cells
                        hits_count = sum(1 for i in hits_indices if i < len(row) and row[i] and row[i].strip())
                        hits_counts.append(hits_count)
                    except Exception:
                        continue
                    
                    if line_count % 100000 == 0:
                        print(f"Processed {line_count} lines...")
        
        end_time = time.time()
        print(f"Analysis completed in {end_time - start_time:.2f} seconds")
        print(f"Collected {len(hits_counts)} hit counts")
        
        return {
            'file_name': os.path.basename(file_path),
            'counts': np.array(hits_counts)
        }
    
    except Exception as e:
        print(f"Error analyzing {file_path}: {e}")
        return None

def compare_hit_counts(file1_path, file2_path, output_path=None, sample_size=0.01):
    """Compare hit counts between two CSV files"""
    
    print("Analyzing hit counts in CSV files...")
    
    # Count hits per row in both files
    counts1 = count_hits_per_row(file1_path, sample_size)
    counts2 = count_hits_per_row(file2_path, sample_size)
    
    if counts1 is None or counts2 is None:
        print("Error: Could not analyze one or both files")
        return
    
    # Create boxplot comparison
    plt.figure(figsize=(12, 8))
    
    # Basic statistics
    print("\nHit Count Statistics:")
    for counts in [counts1, counts2]:
        print(f"\n{counts['file_name']}:")
        print(f"  Min: {np.min(counts['counts'])}")
        print(f"  Max: {np.max(counts['counts'])}")
        print(f"  Mean: {np.mean(counts['counts']):.2f}")
        print(f"  Median: {np.median(counts['counts'])}")
        print(f"  Std Dev: {np.std(counts['counts']):.2f}")
        
        # Calculate outliers (using 1.5 * IQR method)
        q1 = np.percentile(counts['counts'], 25)
        q3 = np.percentile(counts['counts'], 75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outliers = counts['counts'][(counts['counts'] < lower_bound) | (counts['counts'] > upper_bound)]
        
        print(f"  Outliers: {len(outliers)} ({len(outliers)/len(counts['counts'])*100:.2f}%)")
        print(f"  Outlier range: {np.min(outliers) if len(outliers) > 0 else 'N/A'} - "
              f"{np.max(outliers) if len(outliers) > 0 else 'N/A'}")
    
    # Create the boxplot
    box = plt.boxplot(
        [counts1['counts'], counts2['counts']], 
        patch_artist=True, 
        labels=[counts1['file_name'], counts2['file_name']],
        notch=True,  # Add notch for confidence interval around median
        vert=True,   # Vertical boxplot
        whis=1.5     # Whiskers at 1.5 * IQR
    )
    
    # Color the boxes
    box['boxes'][0].set_facecolor('lightblue')
    box['boxes'][1].set_facecolor('lightgreen')
    
    # Add a scatter plot of the raw data with jitter for better visualization
    # Use small alpha to see density
    x_jitter = np.random.normal(0, 0.04, size=len(counts1['counts']))
    plt.scatter(np.repeat(1, len(counts1['counts'])) + x_jitter, counts1['counts'], 
                alpha=0.1, s=3, c='blue')
    
    x_jitter = np.random.normal(0, 0.04, size=len(counts2['counts']))
    plt.scatter(np.repeat(2, len(counts2['counts'])) + x_jitter, counts2['counts'], 
                alpha=0.1, s=3, c='green')
    
    # Add mean as a marker
    plt.plot(1, np.mean(counts1['counts']), 'D', color='red', markersize=8)
    plt.plot(2, np.mean(counts2['counts']), 'D', color='red', markersize=8)
    
    # Add title and labels
    plt.title('Comparison of Hit Counts per Row', fontsize=16)
    plt.ylabel('Number of Hits', fontsize=14)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Add annotations
    for i, counts in enumerate([counts1, counts2], 1):
        plt.text(i, np.max(counts['counts']) * 1.05, 
                 f"Mean: {np.mean(counts['counts']):.1f}\nMedian: {np.median(counts['counts'])}", 
                 horizontalalignment='center', size='small', weight='semibold')
    
    # Save or show the plot
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to {output_path}")
    
    plt.show()
    
    # Additional visualization: histogram comparison
    plt.figure(figsize=(14, 6))
    
    # Plot histograms
    plt.subplot(1, 2, 1)
    sns.histplot(counts1['counts'], kde=True, color='lightblue')
    plt.title(f"Distribution of Hits in {counts1['file_name']}", fontsize=14)
    plt.xlabel('Number of Hits')
    plt.ylabel('Frequency')
    
    plt.subplot(1, 2, 2)
    sns.histplot(counts2['counts'], kde=True, color='lightgreen')
    plt.title(f"Distribution of Hits in {counts2['file_name']}", fontsize=14)
    plt.xlabel('Number of Hits')
    plt.ylabel('Frequency')
    
    plt.tight_layout()
    
    # Save or show the histogram plot
    if output_path:
        histogram_path = os.path.splitext(output_path)[0] + "_histogram.png"
        plt.savefig(histogram_path, dpi=300, bbox_inches='tight')
        print(f"Histogram saved to {histogram_path}")
    
    plt.show()

# Run the script directly with your files
if __name__ == "__main__":
    # Use direct file paths to avoid input prompts
    file1 = "visitas_expandidas_completo (1).csv"
    file2 = "visitas_muchos_hits.csv"
    output_path = "hit_counts_comparison.png"
    
    # Use a very small sample size for these large files
    sample_size = 0.01  # 1% of data
    
    print("Starting CSV hit count comparison...")
    compare_hit_counts(file1, file2, output_path, sample_size)