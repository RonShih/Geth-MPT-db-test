// +build ignore

// LevelDB Demo - 可執行的 MPT → LevelDB 示範程式
// 使用方式:
//   cd /Users/ron/Desktop/Geth-MPT-db-test
//   go run trie/leveldb_demo.go

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb/database"
)

func main() {
	fmt.Println("=== MPT LevelDB Demo ===\n")

	// 創建臨時目錄
	tmpDir, err := os.MkdirTemp("", "mpt-leveldb-demo-*")
	if err != nil {
		fmt.Printf("❌ Failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "demo.db")
	fmt.Printf("📁 Database: %s\n\n", dbPath)

	// 創建 LevelDB
	ldb, err := leveldb.New(dbPath, 128, 128, "", false)
	if err != nil {
		fmt.Printf("❌ Failed to create LevelDB: %v\n", err)
		os.Exit(1)
	}
	diskdb := rawdb.NewDatabase(ldb)

	// 創建測試數據庫 (使用內部的 testDb 結構)
	triedb := newSimpleTestDB(diskdb, rawdb.HashScheme)

	// Demo 1: 基本寫入和讀取
	fmt.Println("📝 Demo 1: Basic Write & Read")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━")
	tr1 := trie.NewEmpty(triedb)
	updateString(tr1, "name", "Alice")
	updateString(tr1, "age", "30")
	updateString(tr1, "city", "Tokyo")

	root1, nodes1 := tr1.Commit(false)
	triedb.Update(root1, types.EmptyRootHash, trienode.NewWithNodeSet(nodes1))
	triedb.Commit(root1)

	fmt.Printf("✓ Wrote 3 key-value pairs\n")
	fmt.Printf("  Root: %s\n\n", root1.Hex()[:16]+"...")

	// 讀取驗證
	tr2, _ := trie.New(trie.TrieID(root1), triedb)
	fmt.Println("Reading back:")
	fmt.Printf("  name: %s\n", getString(tr2, "name"))
	fmt.Printf("  age:  %s\n", getString(tr2, "age"))
	fmt.Printf("  city: %s\n\n", getString(tr2, "city"))

	// Demo 2: 更新和刪除
	fmt.Println("🔄 Demo 2: Update & Delete")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━")
	tr3, _ := trie.New(trie.TrieID(root1), triedb)
	updateString(tr3, "age", "31")               // 修改
	deleteString(tr3, "city")                    // 刪除
	updateString(tr3, "email", "alice@test.com") // 新增

	root2, nodes2 := tr3.Commit(false)
	triedb.Update(root2, root1, trienode.NewWithNodeSet(nodes2))
	triedb.Commit(root2)

	fmt.Printf("✓ Updated age, deleted city, added email\n")
	fmt.Printf("  New Root: %s\n\n", root2.Hex()[:16]+"...")

	// 驗證更新
	tr4, _ := trie.New(trie.TrieID(root2), triedb)
	fmt.Println("After update:")
	fmt.Printf("  name:  %s\n", getString(tr4, "name"))
	fmt.Printf("  age:   %s (updated)\n", getString(tr4, "age"))
	fmt.Printf("  city:  %s (deleted)\n", getString(tr4, "city"))
	fmt.Printf("  email: %s (new)\n\n", getString(tr4, "email"))

	// Demo 3: 歷史版本
	fmt.Println("🕒 Demo 3: Historical Versions")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━")
	trOld, _ := trie.New(trie.TrieID(root1), triedb)
	trNew, _ := trie.New(trie.TrieID(root2), triedb)

	fmt.Println("Version 1 (original):")
	fmt.Printf("  age:  %s\n", getString(trOld, "age"))
	fmt.Printf("  city: %s\n", getString(trOld, "city"))

	fmt.Println("\nVersion 2 (current):")
	fmt.Printf("  age:  %s\n", getString(trNew, "age"))
	fmt.Printf("  city: %s\n\n", getString(trNew, "city"))

	// Demo 4: 使用哈希鍵（模擬以太坊）
	fmt.Println("🔐 Demo 4: Hash Keys (Ethereum-style)")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━")
	tr5 := trie.NewEmpty(triedb)

	accounts := []string{"0xAlice", "0xBob", "0xCharlie"}
	for _, account := range accounts {
		key := crypto.Keccak256([]byte(account))
		value := []byte(account + "_balance:1000ETH")
		tr5.MustUpdate(key, value)
		fmt.Printf("  ✓ Stored %s\n", account)
	}

	root3, nodes3 := tr5.Commit(false)
	triedb.Update(root3, types.EmptyRootHash, trienode.NewWithNodeSet(nodes3))
	triedb.Commit(root3)
	fmt.Printf("\n  Root: %s\n\n", root3.Hex()[:16]+"...")

	// 驗證哈希鍵讀取
	tr6, _ := trie.New(trie.TrieID(root3), triedb)
	fmt.Println("Retrieving by hash key:")
	for _, account := range accounts {
		key := crypto.Keccak256([]byte(account))
		value, _ := tr6.Get(key)
		fmt.Printf("  %s: %s\n", account, string(value))
	}

	diskdb.Close()

	fmt.Println("\n✅ All demos completed successfully!")
	fmt.Printf("📊 Total roots created: 3\n")
	fmt.Printf("🗂️  Database was at: %s (auto-cleaned)\n", dbPath)
}

// Helper functions
func getString(t *trie.Trie, k string) []byte {
	return t.MustGet([]byte(k))
}

func updateString(t *trie.Trie, k, v string) {
	t.MustUpdate([]byte(k), []byte(v))
}

func deleteString(t *trie.Trie, k string) {
	t.MustDelete([]byte(k))
}

// Simple test database implementation (copied from database_test.go)
type simpleTestDB struct {
	disk    ethdb.Database
	root    common.Hash
	scheme  string
	nodes   map[common.Hash]*trienode.MergedNodeSet
	parents map[common.Hash]common.Hash
}

func newSimpleTestDB(diskdb ethdb.Database, scheme string) *simpleTestDB {
	return &simpleTestDB{
		disk:    diskdb,
		root:    types.EmptyRootHash,
		scheme:  scheme,
		nodes:   make(map[common.Hash]*trienode.MergedNodeSet),
		parents: make(map[common.Hash]common.Hash),
	}
}

func (db *simpleTestDB) NodeReader(stateRoot common.Hash) (database.NodeReader, error) {
	nodes, _ := db.dirties(stateRoot, true)
	return &simpleTestReader{db: db.disk, scheme: db.scheme, nodes: nodes}, nil
}

type simpleTestReader struct {
	db     ethdb.Database
	scheme string
	nodes  []*trienode.MergedNodeSet
}

func (r *simpleTestReader) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	for _, nodes := range r.nodes {
		if _, ok := nodes.Sets[owner]; !ok {
			continue
		}
		n, ok := nodes.Sets[owner].Nodes[string(path)]
		if !ok {
			continue
		}
		if n.IsDeleted() || n.Hash != hash {
			return nil, &trie.MissingNodeError{Owner: owner, Path: path, NodeHash: hash}
		}
		return n.Blob, nil
	}
	return rawdb.ReadTrieNode(r.db, owner, path, hash, r.scheme), nil
}

func (db *simpleTestDB) Preimage(hash common.Hash) []byte {
	return rawdb.ReadPreimage(db.disk, hash)
}

func (db *simpleTestDB) InsertPreimage(preimages map[common.Hash][]byte) {
	rawdb.WritePreimages(db.disk, preimages)
}

func (db *simpleTestDB) PreimageEnabled() bool {
	return true
}

func (db *simpleTestDB) Scheme() string { return db.scheme }

func (db *simpleTestDB) Update(root common.Hash, parent common.Hash, nodes *trienode.MergedNodeSet) error {
	if root == parent {
		return nil
	}
	if _, ok := db.nodes[root]; ok {
		return nil
	}
	db.parents[root] = parent
	db.nodes[root] = nodes
	return nil
}

func (db *simpleTestDB) Commit(root common.Hash) error {
	nodes, roots := db.dirties(root, false)
	for i := 0; i < len(nodes); i++ {
		for owner, set := range nodes[i].Sets {
			for path, node := range set.Nodes {
				if node.IsDeleted() {
					rawdb.DeleteTrieNode(db.disk, owner, []byte(path), node.Hash, db.scheme)
				} else {
					rawdb.WriteTrieNode(db.disk, owner, []byte(path), node.Hash, node.Blob, db.scheme)
				}
			}
		}
		delete(db.nodes, roots[i])
	}
	db.root = root
	return nil
}

func (db *simpleTestDB) dirties(root common.Hash, topToBottom bool) ([]*trienode.MergedNodeSet, []common.Hash) {
	var (
		pending []*trienode.MergedNodeSet
		roots   []common.Hash
	)
	for root != db.root {
		nodes, ok := db.nodes[root]
		if !ok {
			break
		}
		if topToBottom {
			pending = append(pending, nodes)
			roots = append(roots, root)
		} else {
			pending = append([]*trienode.MergedNodeSet{nodes}, pending...)
			roots = append([]common.Hash{root}, roots...)
		}
		root = db.parents[root]
	}
	return pending, roots
}
