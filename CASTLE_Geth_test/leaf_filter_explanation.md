# Leaf Node 過濾機制詳解

## 在 analyze_leveldb_ops.py 中如何過濾出 Leaf Node

### 步驟概覽

```
CSV 中所有記錄
    ↓
1. 過濾出 Type == 'PATH_ACCOUNT_TRIE'
    ↓
2. 對每一行應用 is_leaf_node() 函數
    ↓
3. 保留返回 True 的記錄
    ↓
baseline_leveldb_operations.csv (只包含 leaf nodes)
```

---

## 詳細步驟

### 步驟 1: 過濾 PATH_ACCOUNT_TRIE 類型

```python
# 從 CSV 中過濾出所有 PATH_ACCOUNT_TRIE 記錄
account_trie_df = df[df['Type'] == 'PATH_ACCOUNT_TRIE'].copy()
```

**為什麼只看 PATH_ACCOUNT_TRIE？**
- 因為我們關注的是 **account trie 的 leaf nodes**
- PATH_ACCOUNT_TRIE 包含：
  - Branch nodes (fullNode, 17 elements)
  - Extension nodes (shortNode with hashNode value)
  - **Leaf nodes (shortNode with valueNode value)** ← 我們要的！

---

### 步驟 2: 應用 is_leaf_node() 過濾器

```python
leaf_mask = account_trie_df.apply(
    lambda row: is_leaf_node(row['KeyHex'], row['ValueHex']),
    axis=1
)

leaf_df = account_trie_df[leaf_mask].copy()
```

這會對每一行調用 `is_leaf_node()` 函數，返回 True/False 數組。

---

### 步驟 3: is_leaf_node() 函數的內部邏輯

```python
def is_leaf_node(key_hex, value_hex):
    """判斷是否為 leaf node"""

    # 1. 檢查數據有效性
    if pd.isna(value_hex) or value_hex == '':
        return False

    # 2. 解析 hex 字符串
    key_bytes = to_bytes(hexstr=key_hex)
    value_bytes = to_bytes(hexstr=value_hex)

    # 3. 檢查 key 是否以 'A' (0x41) 開頭
    if len(key_bytes) == 0 or key_bytes[0] != 0x41:
        return False

    # 4. RLP 解碼 value
    try:
        node = rlp_decode(value_bytes)
    except:
        return False  # RLP 解碼失敗

    # 5. 檢查是否為 shortNode (2 elements)
    if not is_list(node) or len(node) != 2:
        return False  # 可能是 fullNode (17 elements)

    # 6. 解碼 compact encoding 中的 flag
    node_key = node[0]
    _, is_leaf = decode_compact_encoding(node_key)

    # 7. 返回結果
    return is_leaf
```

---

## 核心判斷：decode_compact_encoding()

```python
def decode_compact_encoding(key):
    """
    解碼 compact encoding，提取 flag 信息

    Compact encoding format (第一個 byte):
    ┌─────────┬─────────┐
    │ 4 bits  │ 4 bits  │
    │  flag   │  data   │
    └─────────┴─────────┘

    Flag bits:
    - bit 1 (0x2): 1 = leaf, 0 = extension
    - bit 0 (0x1): 1 = odd length, 0 = even length
    """
    if len(key) == 0:
        return [], False

    first_byte = key[0]
    flag = first_byte >> 4  # 取高 4 bits

    # 關鍵判斷！
    is_leaf = (flag & 2) != 0  # 檢查 bit 1

    # ... 提取 nibbles ...

    return nibbles, is_leaf
```

---

## 實際例子

### Example 1: Leaf Node (會被保留)

```
CSV Row:
  Operation: PUT
  KeyHex: 0x41060c
  ValueHex: 0xf869a0209d57be05dd69371c4dd2e871bce6e9f4124236825bb612ee18a45e5675be51...
  Type: PATH_ACCOUNT_TRIE

過濾過程:
1. Type == 'PATH_ACCOUNT_TRIE' ✓
2. key_bytes[0] == 0x41 ✓
3. RLP decode:
   node = [
     0x209d57be05dd69371c4dd2e871bce6e9f4124236825bb612ee18a45e5675be51,  # compact key
     0xf8440180a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421...  # account data
   ]
4. len(node) == 2 ✓ (shortNode)
5. node[0][0] = 0x20
   flag = 0x20 >> 4 = 2 (0b0010)
   is_leaf = (2 & 2) != 0 = True ✓

結果: 保留 (寫入 baseline_leveldb_operations.csv)
```

### Example 2: Extension Node (會被過濾掉)

```
假設 CSV Row:
  KeyHex: 0x4101
  ValueHex: 0xd88301a0abcdef1234567890...  # RLP([compact_key, hash])
  Type: PATH_ACCOUNT_TRIE

過濾過程:
1. Type == 'PATH_ACCOUNT_TRIE' ✓
2. key_bytes[0] == 0x41 ✓
3. RLP decode:
   node = [
     0x01abcd...,  # compact key (starts with 0x00 or 0x10)
     0xabcdef1234567890...  # 32-byte hash (hashNode)
   ]
4. len(node) == 2 ✓ (shortNode)
5. node[0][0] = 0x00 (或 0x10)
   flag = 0x00 >> 4 = 0 (0b0000)
   is_leaf = (0 & 2) != 0 = False ✗

結果: 過濾掉 (不寫入 baseline CSV)
```

### Example 3: Full Node / Branch Node (會被過濾掉)

```
假設 CSV Row:
  KeyHex: 0x41
  ValueHex: 0xf8... (RLP of 17-element array)
  Type: PATH_ACCOUNT_TRIE

過濾過程:
1. Type == 'PATH_ACCOUNT_TRIE' ✓
2. key_bytes[0] == 0x41 ✓
3. RLP decode:
   node = [hash0, hash1, hash2, ..., hash15, value] (17 elements)
4. len(node) == 2? ✗ (len = 17)

結果: 過濾掉 (不是 shortNode)
```

---

## 總結

### 過濾條件 (必須全部滿足)

1. ✓ Type == 'PATH_ACCOUNT_TRIE'
2. ✓ Key 以 'A' (0x41) 開頭
3. ✓ Value 可以被 RLP 解碼
4. ✓ RLP 解碼後是 2 元素數組 (shortNode)
5. ✓ **Compact encoding flag 的 bit 1 為 1** (is_leaf)

### Flag 值對照表

| Flag Value | Binary | Type      | Kept? |
|-----------|--------|-----------|-------|
| 0x00      | 0000   | Extension, even | ✗ |
| 0x10      | 0001   | Extension, odd  | ✗ |
| **0x20**  | **0010** | **Leaf, even** | **✓** |
| **0x30**  | **0011** | **Leaf, odd**  | **✓** |

### 結果

只有 **flag & 2 != 0** 的 shortNode 才會被保留到 `baseline_leveldb_operations.csv`，這些就是包含實際賬戶數據的 leaf nodes！
