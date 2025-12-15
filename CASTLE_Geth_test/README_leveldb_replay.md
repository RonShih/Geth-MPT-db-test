# LevelDB Replay Tool

這個工具可以讀取 baseline 或 MPT 的 CSV 檔案，將操作重放到 LevelDB，並顯示詳細的統計資訊。

## 功能

- 讀取 `baseline_leveldb_operations.csv` 或 `MPT_leveldb_operations.csv`
- 重放所有 GET/PUT/DELETE 操作到 LevelDB
- 顯示操作統計（ops 數量、bytes 數量、吞吐量）
- 顯示 LevelDB 內部統計（compaction、cache、等）
- 顯示磁碟使用量

## 編譯

```bash
cd CASTLE_Geth_test
go build leveldb_replay.go
```

## 使用方法

### 基本用法

```bash
# Replay baseline (只有 leaf nodes)
go run leveldb_replay.go -csv baseline_leveldb_operations.csv -db ./baseline_db -clean

# Replay MPT (所有 PATH_ACCOUNT_TRIE nodes)
go run leveldb_replay.go -csv MPT_leveldb_operations.csv -db ./mpt_db -clean
```

### 參數說明

- `-csv <file>` - CSV 檔案路徑（必需）
  - `baseline_leveldb_operations.csv` - 只有 leaf nodes
  - `MPT_leveldb_operations.csv` - 完整的 MPT 節點

- `-db <path>` - LevelDB 資料庫路徑（必需）
  - 如果不存在會自動創建

- `-clean` - 在開始前清空資料庫（可選）
  - 建議每次重放時使用

## 輸出範例

```
================================================================================
LevelDB Replay Tool
================================================================================
CSV File: baseline_leveldb_operations.csv
Dataset Type: Baseline (Leaf Nodes Only)
LevelDB Path: ./baseline_db
Clean Database: true

Opening LevelDB...
Reading CSV file...
Found 1500 operations in CSV

Replaying operations to LevelDB...
Processed 1500 / 1500 operations... Done!

================================================================================
Operation Statistics
================================================================================
Total Operations: 1500
  GET: 500
  PUT: 950
  DELETE: 50

Total Bytes Written: 102400 (0.10 MB)
Total Bytes Read: 51200 (0.05 MB)

Duration: 123ms
Throughput: 12195.12 ops/sec

================================================================================
LevelDB Statistics
================================================================================
Compactions
 Level |   Tables   |    Size(MB)   |    Time(sec)  |    Read(MB)   |   Write(MB)
-------+------------+---------------+---------------+---------------+---------------
   0   |          1 |         0.098 |         0.001 |         0.000 |         0.098
   1   |          0 |         0.000 |         0.000 |         0.000 |         0.000
   2   |          0 |         0.000 |         0.000 |         0.000 |         0.000

================================================================================
Storage Information
================================================================================
Database Size on Disk: 105472 bytes (0.10 MB)
Total Keys in Database: 950

Replay completed successfully!
```

## 比較 Baseline vs MPT

### 步驟 1: Replay Baseline
```bash
go run leveldb_replay.go -csv baseline_leveldb_operations.csv -db ./baseline_db -clean
```

記錄輸出的統計：
- Total Operations
- Bytes Written/Read
- Database Size on Disk
- Total Keys

### 步驟 2: Replay MPT
```bash
go run leveldb_replay.go -csv MPT_leveldb_operations.csv -db ./mpt_db -clean
```

記錄輸出的統計（同上）

### 步驟 3: 比較結果

計算 MPT overhead:
- `MPT Operations / Baseline Operations` - 操作數量倍數
- `MPT Bytes / Baseline Bytes` - 數據量倍數
- `MPT DB Size / Baseline DB Size` - 磁碟空間倍數

## 注意事項

1. **CSV 格式要求**
   - 必須包含 header: `Operation,KeyHex,ValueHex,KeySize,ValueSize,Type`
   - KeyHex 和 ValueHex 必須是有效的 hex 字符串（可以有或沒有 "0x" 前綴）

2. **資料庫路徑**
   - 建議每次測試使用不同的資料庫路徑
   - 或使用 `-clean` flag 清空現有資料庫

3. **記憶體使用**
   - 工具會將整個 CSV 讀入記憶體
   - 對於大型 CSV 檔案，注意記憶體使用量

4. **GET 操作**
   - GET 操作可能會失敗（key 不存在）
   - 這是正常的，不會影響統計

## LevelDB 統計說明

### Compactions
- **Level**: SSTable 的層級（0-6）
- **Tables**: 該層級的 table 數量
- **Size**: 該層級的總大小
- **Time**: Compaction 耗時
- **Read/Write**: Compaction 的讀寫量

### 其他統計
- **Database Size on Disk**: 資料庫在磁碟上的實際大小
- **Total Keys**: 資料庫中的 key 總數

## 疑難排解

### 錯誤: "CSV file is empty or has no data rows"
- 檢查 CSV 檔案是否存在
- 確認 CSV 有 header 和至少一行數據

### 錯誤: "Failed to decode key/value"
- 檢查 CSV 中的 hex 字符串格式
- 確認沒有無效字符

### 資料庫已存在
- 使用 `-clean` flag 清空
- 或手動刪除資料庫目錄
