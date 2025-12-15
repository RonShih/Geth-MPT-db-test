#!/usr/bin/env python3
"""
Extract account trie leaf nodes from leveldb_operations.csv.
Reconstructs account data from PATH_ACCOUNT_TRIE leaf nodes.
"""

import pandas as pd
import os
from rlp import decode as rlp_decode
from eth_utils import to_bytes, to_hex, keccak

def is_list(data):
    """Check if RLP decoded data is a list"""
    return isinstance(data, list)

def nibbles_to_bytes(nibbles):
    """Convert hex nibbles to bytes (2 nibbles = 1 byte)"""
    if len(nibbles) % 2 != 0:
        return None  # Odd length, cannot be complete leaf
    result = bytearray()
    for i in range(0, len(nibbles), 2):
        byte = (nibbles[i] << 4) | nibbles[i+1]
        result.append(byte)
    return bytes(result)

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

def analyze_account_leaves(csv_path):
    """
    Extract and analyze account trie leaf nodes from CSV.
    """
    print(f"Reading CSV file: {csv_path}")
    df = pd.read_csv(csv_path)

    # Filter PATH_ACCOUNT_TRIE entries
    account_trie_df = df[df['Type'] == 'PATH_ACCOUNT_TRIE']

    print(f"\nTotal PATH_ACCOUNT_TRIE operations: {len(account_trie_df)}")

    leaves = []

    for idx, row in account_trie_df.iterrows():
        try:
            # Parse key and value
            key_hex = row['KeyHex']
            value_hex = row['ValueHex']

            if pd.isna(value_hex) or value_hex == '':
                continue

            key_bytes = to_bytes(hexstr=key_hex)
            value_bytes = to_bytes(hexstr=value_hex)

            # Check if key starts with 'A' (0x41)
            if len(key_bytes) == 0 or key_bytes[0] != 0x41:
                continue

            # Extract path (remove 'A' prefix)
            path = key_bytes[1:]

            # Decode RLP
            try:
                node = rlp_decode(value_bytes)
            except:
                continue

            # Check if it's a shortNode (2 elements: [key, value])
            if not is_list(node) or len(node) != 2:
                continue

            node_key = node[0]
            node_val = node[1]

            # Decode compact encoding
            key_nibbles, is_leaf = decode_compact_encoding(node_key)

            if not is_leaf:
                continue  # This is an extension node, not a leaf

            # This is a leaf node!
            # Reconstruct full path
            path_nibbles = []
            for byte in path:
                path_nibbles.append(byte >> 4)
                path_nibbles.append(byte & 0x0F)

            full_nibbles = path_nibbles + key_nibbles

            # Convert nibbles to address hash (if even length)
            addr_hash_bytes = nibbles_to_bytes(full_nibbles)
            if addr_hash_bytes is None:
                continue

            addr_hash = to_hex(addr_hash_bytes)

            # Decode account data (RLP encoded StateAccount)
            try:
                account = rlp_decode(node_val)
                if not is_list(account) or len(account) != 4:
                    continue

                nonce = int.from_bytes(account[0], 'big') if account[0] else 0
                balance = int.from_bytes(account[1], 'big') if account[1] else 0
                storage_root = to_hex(account[2])
                code_hash = to_hex(account[3])

                leaves.append({
                    'AddressHash': addr_hash,
                    'Nonce': nonce,
                    'Balance': balance,
                    'StorageRoot': storage_root,
                    'CodeHash': code_hash,
                    'Path': to_hex(path),
                    'Operation': row['Operation']
                })
            except:
                continue

        except Exception as e:
            continue

    print(f"\nFound {len(leaves)} account leaf nodes\n")

    if len(leaves) > 0:
        # Display results
        print("=" * 140)
        print(f"{'AddressHash':<68s} {'Nonce':<8s} {'Balance':<20s} {'Operation':<10s}")
        print("-" * 140)

        for leaf in leaves[:20]:  # Show first 20
            print(f"{leaf['AddressHash']:<68s} {leaf['Nonce']:<8d} {leaf['Balance']:<20d} {leaf['Operation']:<10s}")

        if len(leaves) > 20:
            print(f"... and {len(leaves) - 20} more")

        print("-" * 140)

        # Statistics
        get_count = sum(1 for l in leaves if l['Operation'] == 'GET')
        put_count = sum(1 for l in leaves if l['Operation'] == 'PUT')

        print(f"\nLeaf node operations:")
        print(f"  GET: {get_count}")
        print(f"  PUT: {put_count}")

        total_balance = sum(l['Balance'] for l in leaves)
        print(f"\nTotal balance in leaves: {total_balance} wei ({total_balance / 1e18:.6f} ETH)")

    return leaves


if __name__ == "__main__":
    # Path to CSV file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    geth_dir = os.path.dirname(script_dir)
    csv_path = os.path.join(geth_dir, "leveldb_operations.csv")

    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        print(f"Please run Geth to generate leveldb_operations.csv first")
        exit(1)

    leaves = analyze_account_leaves(csv_path)
