#!/usr/bin/env python3
"""Analyze Trie Statistics - Cumulative and Windowed"""

import pandas as pd
import matplotlib.pyplot as plt

# Configuration
CSV_FILE = "hashdb_trie_stats.csv"
WINDOW_SIZE = 10  # Range size: every 10 MPT_GET operations

def analyze_and_plot():
    # Load data
    df = pd.read_csv(CSV_FILE)
    df = df[df['Operation'] == 'MPT_GET'].reset_index(drop=True)
    print(f"Total MPT_GET operations: {len(df)}")

    # Calculate cumulative statistics
    cumulative = pd.DataFrame({
        'X': range(1, len(df) + 1),
        'ValueNode': df['ValueNodeCount'].cumsum(),
        'ShortNode': df['ShortNodeCount'].cumsum(),
        'FullNode': df['FullNodeCount'].cumsum(),
        'HashNode': df['HashNodeCount'].cumsum(),
        'ValueNodeBytes': df['ValueNodeBytes'].cumsum(),
        'ShortNodeBytes': df['ShortNodeBytes'].cumsum(),
        'FullNodeBytes': df['FullNodeBytes'].cumsum(),
        'DBDiskRead': df['DBDiskRead'].cumsum(),
    })

    # Calculate windowed statistics
    num_windows = len(df) // WINDOW_SIZE
    windows = []

    for i in range(num_windows):
        start, end = i * WINDOW_SIZE, (i + 1) * WINDOW_SIZE
        window = df.iloc[start:end]
        windows.append({
            'X': (i + 1) * WINDOW_SIZE,
            'ValueNode': window['ValueNodeCount'].sum(),
            'ShortNode': window['ShortNodeCount'].sum(),
            'FullNode': window['FullNodeCount'].sum(),
            'HashNode': window['HashNodeCount'].sum(),
            'ValueNodeBytes': window['ValueNodeBytes'].sum(),
            'ShortNodeBytes': window['ShortNodeBytes'].sum(),
            'FullNodeBytes': window['FullNodeBytes'].sum(),
            'DBDiskRead': window['DBDiskRead'].sum(),
        })

    wdf = pd.DataFrame(windows)

    # Plot 1: Node Counts
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # Cumulative
    for node in ['ValueNode', 'ShortNode', 'FullNode', 'HashNode']:
        ax1.plot(cumulative['X'], cumulative[node], linewidth=2, label=node)
    ax1.set_xlabel('MPT_GET Operation Number', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Cumulative Count', fontsize=12, fontweight='bold')
    ax1.set_title('Cumulative Node Counts', fontsize=14, fontweight='bold')
    ax1.legend(loc='upper left', fontsize=10)
    ax1.grid(True, alpha=0.3)

    # Windowed
    for node in ['ValueNode', 'ShortNode', 'FullNode', 'HashNode']:
        ax2.plot(wdf['X'], wdf[node], marker='o', linestyle='', markersize=5, label=node)
    ax2.set_xlabel('MPT_GET Operation Number', fontsize=12, fontweight='bold')
    ax2.set_ylabel(f'Total Count per {WINDOW_SIZE} ops', fontsize=12, fontweight='bold')
    ax2.set_title(f'Node Counts per {WINDOW_SIZE} Operations', fontsize=14, fontweight='bold')
    ax2.legend(loc='upper left', fontsize=10)
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('node_counts.png', dpi=300, bbox_inches='tight')
    print("Saved: node_counts.png")
    plt.close()

    # Plot 2: Bytes
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # Cumulative (in MB)
    byte_cols = ['ValueNodeBytes', 'ShortNodeBytes', 'FullNodeBytes', 'DBDiskRead']
    byte_labels = ['ValueNode', 'ShortNode', 'FullNode', 'HashNode']
    for col, label in zip(byte_cols, byte_labels):
        ax1.plot(cumulative['X'], cumulative[col]/1024/1024, linewidth=2, label=label)
    ax1.set_xlabel('MPT_GET Operation Number', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Cumulative Bytes (MB)', fontsize=12, fontweight='bold')
    ax1.set_title('Cumulative Byte Counts', fontsize=14, fontweight='bold')
    ax1.legend(loc='upper left', fontsize=10)
    ax1.grid(True, alpha=0.3)

    # Windowed (in KB)
    for col, label in zip(byte_cols, byte_labels):
        ax2.plot(wdf['X'], wdf[col]/1024, marker='o', linestyle='', markersize=5, label=label)
    ax2.set_xlabel('MPT_GET Operation Number', fontsize=12, fontweight='bold')
    ax2.set_ylabel(f'Total Bytes per {WINDOW_SIZE} ops (KB)', fontsize=12, fontweight='bold')
    ax2.set_title(f'Byte Counts per {WINDOW_SIZE} Operations', fontsize=14, fontweight='bold')
    ax2.legend(loc='upper left', fontsize=10)
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('byte_counts.png', dpi=300, bbox_inches='tight')
    print("Saved: byte_counts.png")
    plt.close()

    # Print summary
    print(f"\n{'='*70}")
    print("SUMMARY STATISTICS")
    print(f"{'='*70}")
    print(f"Total Operations: {len(df)}")
    print(f"Window Size: {WINDOW_SIZE} operations")
    print(f"Number of Windows: {num_windows}")

    print(f"\n{'-'*70}")
    print("FINAL CUMULATIVE TOTALS")
    print(f"{'-'*70}")
    print(f"{'Node Type':<15} {'Total Count':>20}")
    print(f"{'-'*70}")
    for node in ['ValueNode', 'ShortNode', 'FullNode', 'HashNode']:
        total = df[f'{node}Count'].sum()
        print(f"{node:<15} {total:>20,}")

    print(f"\n{'Byte Type':<15} {'Total Bytes':>20} {'Total (MB)':>20}")
    print(f"{'-'*70}")
    for col in ['ValueNodeBytes', 'ShortNodeBytes', 'FullNodeBytes', 'DBDiskRead']:
        col_name = col.replace('NodeBytes', '').replace('DBDiskRead', 'DiskRead')
        total = df[col].sum()
        print(f"{col_name:<15} {total:>20,} {total/1024/1024:>20.2f}")

    print(f"{'='*70}\n")

if __name__ == "__main__":
    analyze_and_plot()
