#!/usr/bin/env python3
"""
Analyze LevelDB operations from CSV file.
Counts operations by type (GET/PUT/DELETE) and by key prefix (A/a/O/o).
Also extracts leaf nodes for baseline analysis.
"""

import pandas as pd
import os
from rlp import decode as rlp_decode
from eth_utils import to_bytes

def is_list(data):
    """Check if RLP decoded data is a list"""
    return isinstance(data, list)

def decode_compact_encoding(key):
    """
    Decode compact encoding used in trie keys.
    Returns (nibbles, is_leaf)
    """
    if len(key) == 0:
        return [], False

    first_byte = key[0]
    flag = first_byte >> 4  # High 4 bits
    is_leaf = (flag & 2) != 0

    # Extract nibbles
    nibbles = []
    if flag & 1:  # Odd length
        nibbles.append(first_byte & 0x0F)

    for byte in key[1:]:
        nibbles.append(byte >> 4)
        nibbles.append(byte & 0x0F)

    return nibbles, is_leaf

def is_leaf_node(key_hex, value_hex):
    """
    Check if a PATH_ACCOUNT_TRIE entry is a leaf node.
    Returns True if it's a leaf node.
    """
    try:
        if pd.isna(value_hex) or value_hex == '':
            return False

        key_bytes = to_bytes(hexstr=key_hex)
        value_bytes = to_bytes(hexstr=value_hex)

        # Check if key starts with 'A' (0x41)
        if len(key_bytes) == 0 or key_bytes[0] != 0x41:
            return False

        # Decode RLP
        try:
            node = rlp_decode(value_bytes)
        except:
            return False

        # Check if it's a shortNode (2 elements: [key, value])
        if not is_list(node) or len(node) != 2:
            return False

        node_key = node[0]

        # Decode compact encoding
        _, is_leaf = decode_compact_encoding(node_key)

        return is_leaf

    except:
        return False

def analyze_leveldb_operations(csv_path):
    """
    Analyze LevelDB operations CSV file.

    Args:
        csv_path: Path to leveldb_operations.csv
    """
    print(f"Reading CSV file: {csv_path}")
    df = pd.read_csv(csv_path)

    # CSV format: Operation,KeyHex,ValueHex,KeySize,ValueSize,Type
    # Type field contains: PATH_ACCOUNT_TRIE, PATH_STORAGE_TRIE, HASH_TRIE, SNAPSHOT_ACCOUNT, SNAPSHOT_STORAGE

    # Extract leaf nodes for baseline
    print("\nExtracting leaf nodes for baseline analysis...")
    account_trie_df = df[df['Type'] == 'PATH_ACCOUNT_TRIE'].copy()

    leaf_mask = account_trie_df.apply(
        lambda row: is_leaf_node(row['KeyHex'], row['ValueHex']),
        axis=1
    )

    leaf_df = account_trie_df[leaf_mask].copy()

    print(f"Found {len(leaf_df)} leaf node operations out of {len(account_trie_df)} PATH_ACCOUNT_TRIE operations")

    # Write MPT CSV (all PATH_ACCOUNT_TRIE operations)
    mpt_csv_path = csv_path.replace('leveldb_operations.csv', 'MPT_leveldb_operations.csv')
    account_trie_df.to_csv(mpt_csv_path, index=False)
    print(f"MPT operations written to: {mpt_csv_path}")

    # Write baseline CSV (leaf nodes only)
    baseline_csv_path = csv_path.replace('leveldb_operations.csv', 'baseline_leveldb_operations.csv')
    leaf_df.to_csv(baseline_csv_path, index=False)
    print(f"Baseline leaf operations written to: {baseline_csv_path}")

    # Summary table - Combined Counts and Value Size
    print("\n" + "=" * 120)
    print("Summary Table (by Type):")
    print("=" * 120)
    print(f"{'Type':<25s} {'Description':<25s} {'GET':>23s} {'PUT':>23s} {'DELETE':>10s}")
    print("-" * 120)

    type_mapping = {
        'PATH_ACCOUNT_TRIE': 'Path-Scheme Account Trie',
        'PATH_STORAGE_TRIE': 'Path-Scheme Storage Trie',
        'HASH_TRIE': 'Hash-Scheme Trie Node',
        'SNAPSHOT_ACCOUNT': 'Snapshot Account',
        'SNAPSHOT_STORAGE': 'Snapshot Storage'
    }

    for type_name, desc in type_mapping.items():
        # Count operations
        get_count = len(df[(df['Operation'] == 'GET') & (df['Type'] == type_name)])
        put_count = len(df[(df['Operation'] == 'PUT') & (df['Type'] == type_name)])
        del_count = len(df[(df['Operation'] == 'DELETE') & (df['Type'] == type_name)])

        # Calculate total value size
        get_df = df[(df['Operation'] == 'GET') & (df['Type'] == type_name)]
        put_df = df[(df['Operation'] == 'PUT') & (df['Type'] == type_name)]

        get_total_bytes = get_df['ValueSize'].sum() if len(get_df) > 0 else 0
        put_total_bytes = put_df['ValueSize'].sum() if len(put_df) > 0 else 0

        # Format: "count (bytes)"
        get_str = f"{get_count:,d} ({get_total_bytes:,d}B)"
        put_str = f"{put_count:,d} ({put_total_bytes:,d}B)"
        del_str = f"{del_count:,d}"

        print(f"{type_name:<25s} {desc:<25s} {get_str:>23s} {put_str:>23s} {del_str:>10s}")

    # Total across all types
    print("-" * 120)
    get_count_total = len(df[df['Operation'] == 'GET'])
    put_count_total = len(df[df['Operation'] == 'PUT'])
    del_count_total = len(df[df['Operation'] == 'DELETE'])
    get_bytes_total = df[df['Operation'] == 'GET']['ValueSize'].sum()
    put_bytes_total = df[df['Operation'] == 'PUT']['ValueSize'].sum()

    get_total_str = f"{get_count_total:,d} ({get_bytes_total:,d}B)"
    put_total_str = f"{put_count_total:,d} ({put_bytes_total:,d}B)"
    del_total_str = f"{del_count_total:,d}"

    print(f"{'TOTAL':<25s} {'All Operations':<25s} {get_total_str:>23s} {put_total_str:>23s} {del_total_str:>10s}")
    print()

    # Baseline statistics - MPT vs Baseline comparison
    print("\n" + "=" * 120)
    print("Baseline Comparison (MPT vs Baseline):")
    print("=" * 120)
    print(f"{'Metric':<50s} {'GET':>23s} {'PUT':>23s} {'DELETE':>10s}")
    print("-" * 120)

    # MPT (PATH_ACCOUNT_TRIE) statistics
    mpt_get_count = len(account_trie_df[account_trie_df['Operation'] == 'GET'])
    mpt_put_count = len(account_trie_df[account_trie_df['Operation'] == 'PUT'])
    mpt_del_count = len(account_trie_df[account_trie_df['Operation'] == 'DELETE'])

    mpt_get_df = account_trie_df[account_trie_df['Operation'] == 'GET']
    mpt_put_df = account_trie_df[account_trie_df['Operation'] == 'PUT']

    mpt_get_bytes = mpt_get_df['ValueSize'].sum() if len(mpt_get_df) > 0 else 0
    mpt_put_bytes = mpt_put_df['ValueSize'].sum() if len(mpt_put_df) > 0 else 0

    mpt_get_str = f"{mpt_get_count:,d} ({mpt_get_bytes:,d}B)"
    mpt_put_str = f"{mpt_put_count:,d} ({mpt_put_bytes:,d}B)"
    mpt_del_str = f"{mpt_del_count:,d}"

    print(f"{'MPT (PATH_ACCOUNT_TRIE)':<50s} {mpt_get_str:>23s} {mpt_put_str:>23s} {mpt_del_str:>10s}")

    # Baseline (Leaf Nodes) statistics
    baseline_get_count = len(leaf_df[leaf_df['Operation'] == 'GET'])
    baseline_put_count = len(leaf_df[leaf_df['Operation'] == 'PUT'])
    baseline_del_count = len(leaf_df[leaf_df['Operation'] == 'DELETE'])

    baseline_get_df = leaf_df[leaf_df['Operation'] == 'GET']
    baseline_put_df = leaf_df[leaf_df['Operation'] == 'PUT']

    baseline_get_bytes = baseline_get_df['ValueSize'].sum() if len(baseline_get_df) > 0 else 0
    baseline_put_bytes = baseline_put_df['ValueSize'].sum() if len(baseline_put_df) > 0 else 0

    baseline_get_str = f"{baseline_get_count:,d} ({baseline_get_bytes:,d}B)"
    baseline_put_str = f"{baseline_put_count:,d} ({baseline_put_bytes:,d}B)"
    baseline_del_str = f"{baseline_del_count:,d}"

    print(f"{'Baseline (Leaf Nodes)':<50s} {baseline_get_str:>23s} {baseline_put_str:>23s} {baseline_del_str:>10s}")
    print("-" * 120)

    # Baseline 佔 MPT 的比例
    if mpt_get_count > 0:
        baseline_get_pct = (baseline_get_count / mpt_get_count) * 100
    else:
        baseline_get_pct = 0

    if mpt_put_count > 0:
        baseline_put_pct = (baseline_put_count / mpt_put_count) * 100
    else:
        baseline_put_pct = 0

    if mpt_del_count > 0:
        baseline_del_pct = (baseline_del_count / mpt_del_count) * 100
    else:
        baseline_del_pct = 0

    if mpt_get_bytes > 0:
        baseline_get_bytes_pct = (baseline_get_bytes / mpt_get_bytes) * 100
    else:
        baseline_get_bytes_pct = 0

    if mpt_put_bytes > 0:
        baseline_put_bytes_pct = (baseline_put_bytes / mpt_put_bytes) * 100
    else:
        baseline_put_bytes_pct = 0

    print(f"{'Baseline % of MPT (count)':<50s} {f'{baseline_get_pct:.2f}%':>23s} {f'{baseline_put_pct:.2f}%':>23s} {f'{baseline_del_pct:.2f}%':>10s}")
    print(f"{'Baseline % of MPT (bytes)':<50s} {f'{baseline_get_bytes_pct:.2f}%':>23s} {f'{baseline_put_bytes_pct:.2f}%':>23s} {'N/A':>10s}")
    print("-" * 120)

    # MPT 比 Baseline 多了多少 (overhead)
    if baseline_get_count > 0:
        mpt_overhead_get_pct = ((mpt_get_count - baseline_get_count) / baseline_get_count) * 100
    else:
        mpt_overhead_get_pct = 0

    if baseline_put_count > 0:
        mpt_overhead_put_pct = ((mpt_put_count - baseline_put_count) / baseline_put_count) * 100
    else:
        mpt_overhead_put_pct = 0

    if baseline_del_count > 0:
        mpt_overhead_del_pct = ((mpt_del_count - baseline_del_count) / baseline_del_count) * 100
    else:
        mpt_overhead_del_pct = 0

    if baseline_get_bytes > 0:
        mpt_overhead_get_bytes_pct = ((mpt_get_bytes - baseline_get_bytes) / baseline_get_bytes) * 100
    else:
        mpt_overhead_get_bytes_pct = 0

    if baseline_put_bytes > 0:
        mpt_overhead_put_bytes_pct = ((mpt_put_bytes - baseline_put_bytes) / baseline_put_bytes) * 100
    else:
        mpt_overhead_put_bytes_pct = 0

    print(f"{'MPT overhead vs Baseline (count)':<50s} {f'+{mpt_overhead_get_pct:.2f}%':>23s} {f'+{mpt_overhead_put_pct:.2f}%':>23s} {f'+{mpt_overhead_del_pct:.2f}%':>10s}")
    print(f"{'MPT overhead vs Baseline (bytes)':<50s} {f'+{mpt_overhead_get_bytes_pct:.2f}%':>23s} {f'+{mpt_overhead_put_bytes_pct:.2f}%':>23s} {'N/A':>10s}")
    print()


if __name__ == "__main__":
    # Path to CSV file (relative to this script's location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    geth_dir = os.path.dirname(script_dir)  # Go up to go-ethereum directory
    csv_path = os.path.join(geth_dir, "leveldb_operations.csv")

    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        print(f"Please run Geth to generate leveldb_operations.csv first")
        exit(1)

    analyze_leveldb_operations(csv_path)
