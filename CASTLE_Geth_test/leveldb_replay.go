package main

import (
	"encoding/csv"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func main() {
	// Command line flags
	csvFile := flag.String("csv", "", "Path to CSV file (baseline_leveldb_operations.csv or MPT_leveldb_operations.csv)")
	dbPath := flag.String("db", "", "Path to LevelDB directory (will be created if not exists)")
	clean := flag.Bool("clean", false, "Clean (delete) existing database before replay")
	flag.Parse()

	if *csvFile == "" || *dbPath == "" {
		fmt.Println("Usage: go run leveldb_replay.go -csv <csv_file> -db <db_path> [-clean]")
		fmt.Println("\nExample:")
		fmt.Println("  go run leveldb_replay.go -csv baseline_leveldb_operations.csv -db ./test_db -clean")
		fmt.Println("  go run leveldb_replay.go -csv MPT_leveldb_operations.csv -db ./test_db -clean")
		os.Exit(1)
	}

	// Clean database if requested
	if *clean {
		if _, err := os.Stat(*dbPath); err == nil {
			if err := os.RemoveAll(*dbPath); err != nil {
				fmt.Printf("Error cleaning database: %v\n", err)
				os.Exit(1)
			}
		}
	}

	// Open LevelDB
	opts := &opt.Options{
		WriteBuffer: 4 * 1024 * 1024, // 4MB write buffer
	}
	db, err := leveldb.OpenFile(*dbPath, opts)
	if err != nil {
		fmt.Printf("Error opening LevelDB: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Read and replay CSV
	records, err := readCSV(*csvFile)
	if err != nil {
		fmt.Printf("Error reading CSV: %v\n", err)
		os.Exit(1)
	}

	// Replay operations (silent)
	_ = replayOperations(db, records)

	// Print all LevelDB statistics
	printLevelDBStats(db, *dbPath)
}

func printLevelDBStats(db *leveldb.DB, dbPath string) {
	fmt.Println(strings.Repeat("=", 100))
	fmt.Println("LevelDB Internal Statistics")
	fmt.Println(strings.Repeat("=", 100))

	// 1. Main stats (includes compaction info)
	stats, err := db.GetProperty("leveldb.stats")
	if err == nil {
		fmt.Println(stats)
	}

	fmt.Println(strings.Repeat("-", 100))

	// 2. SSTable stats per level
	fmt.Println("\nSSTable Statistics by Level:")
	fmt.Println(strings.Repeat("-", 100))
	for i := 0; i < 7; i++ {
		sstables, err := db.GetProperty(fmt.Sprintf("leveldb.sstables.%d", i))
		if err == nil && sstables != "" {
			fmt.Printf("Level %d:\n%s\n", i, sstables)
		}
	}

	fmt.Println(strings.Repeat("-", 100))

	// 3. Number of files at each level
	fmt.Println("\nFiles Per Level:")
	fmt.Println(strings.Repeat("-", 100))
	numFiles, err := db.GetProperty("leveldb.num-files-at-level")
	if err == nil {
		fmt.Println(numFiles)
	}

	fmt.Println(strings.Repeat("-", 100))

	// 4. Approximate memory usage
	fmt.Println("\nMemory Usage:")
	fmt.Println(strings.Repeat("-", 100))

	blockCache, err := db.GetProperty("leveldb.blockcache")
	if err == nil {
		fmt.Printf("Block Cache: %s\n", blockCache)
	}

	approxMem, err := db.GetProperty("leveldb.approximate-memory-usage")
	if err == nil {
		fmt.Printf("Approximate Memory Usage: %s bytes\n", approxMem)
	}

	fmt.Println(strings.Repeat("-", 100))

	// 5. I/O Statistics
	fmt.Println("\nI/O Statistics:")
	fmt.Println(strings.Repeat("-", 100))

	ioStats, err := db.GetProperty("leveldb.iostats")
	if err == nil {
		fmt.Println(ioStats)
	}

	fmt.Println(strings.Repeat("-", 100))

	// 6. Compaction statistics
	fmt.Println("\nCompaction Statistics:")
	fmt.Println(strings.Repeat("-", 100))

	// Total compaction stats
	compStats, err := db.GetProperty("leveldb.compcount")
	if err == nil {
		fmt.Printf("Compaction Count: %s\n", compStats)
	}

	// Write amplification
	writeAmp, err := db.GetProperty("leveldb.write-amp")
	if err == nil {
		fmt.Printf("Write Amplification: %s\n", writeAmp)
	}

	fmt.Println(strings.Repeat("=", 100))

	// 7. Storage Information
	fmt.Println("Storage Information")
	fmt.Println(strings.Repeat("=", 100))

	// Database size on disk
	dbSize, err := getDirSize(dbPath)
	if err == nil {
		fmt.Printf("Database Size on Disk: %d bytes (%.2f MB)\n", dbSize, float64(dbSize)/(1024*1024))
	}

	// Total keys
	iter := db.NewIterator(nil, nil)
	keyCount := 0
	for iter.Next() {
		keyCount++
	}
	iter.Release()
	fmt.Printf("Total Keys in Database: %d\n", keyCount)

	// Size per level (approximate)
	fmt.Println("\nSize Per Level:")
	for i := 0; i < 7; i++ {
		size, err := db.GetProperty(fmt.Sprintf("leveldb.size-bytes-level%d", i))
		if err == nil && size != "" && size != "0" {
			fmt.Printf("  Level %d: %s bytes\n", i, size)
		}
	}
}

// OperationStats holds statistics about replayed operations
type OperationStats struct {
	TotalOps     int
	GetOps       int
	PutOps       int
	DeleteOps    int
	BytesWritten int64
	BytesRead    int64
}

// CSVRecord represents a single row from the CSV
type CSVRecord struct {
	Operation string
	KeyHex    string
	ValueHex  string
	KeySize   string
	ValueSize string
	Type      string
}

func readCSV(filename string) ([]CSVRecord, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(rows) < 2 {
		return nil, fmt.Errorf("CSV file is empty or has no data rows")
	}

	// Skip header row
	records := make([]CSVRecord, 0, len(rows)-1)
	for i := 1; i < len(rows); i++ {
		row := rows[i]
		if len(row) < 6 {
			continue
		}
		records = append(records, CSVRecord{
			Operation: row[0],
			KeyHex:    row[1],
			ValueHex:  row[2],
			KeySize:   row[3],
			ValueSize: row[4],
			Type:      row[5],
		})
	}

	return records, nil
}

func replayOperations(db *leveldb.DB, records []CSVRecord) OperationStats {
	stats := OperationStats{}

	for _, record := range records {
		// Decode key
		key, err := decodeHex(record.KeyHex)
		if err != nil {
			continue
		}

		stats.TotalOps++

		switch record.Operation {
		case "GET":
			stats.GetOps++
			value, err := db.Get(key, nil)
			if err == nil {
				stats.BytesRead += int64(len(value))
			}

		case "PUT":
			stats.PutOps++
			value, err := decodeHex(record.ValueHex)
			if err != nil {
				continue
			}
			err = db.Put(key, value, nil)
			if err != nil {
				continue
			}
			stats.BytesWritten += int64(len(value))

		case "DELETE":
			stats.DeleteOps++
			_ = db.Delete(key, nil)
		}
	}

	return stats
}

func decodeHex(hexStr string) ([]byte, error) {
	// Remove "0x" prefix if present
	hexStr = strings.TrimPrefix(hexStr, "0x")
	return hex.DecodeString(hexStr)
}

func getDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}
