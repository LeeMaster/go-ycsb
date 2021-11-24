package tmdb

import (
	"context"
	"database/sql"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	tm "github.com/tendermint/tm-db"
)

// properties
const (
	// these parameters used for bootstrap a new TMDB instance wrapper
	tmdbName    = "tmdb.name"
	tmdbDir     = "tmdb.dir"
	tmdbBackend = "tmdb.backend" // default the rocksdb will be installed in your own machine

	// tmdb rocksdb options
	tmdbRocksBlockSize                        = "tmdb.rocksdb.block_size"
	tmdbRocksBlockCache                       = "tmdb.rocksdb.block_cache"
	tmdbRocksUseDirectIoForFlushAndCompaction = "tmdb.rocksdb.use_direct_io_for_flush_and_compaction" // bool
	tmdbRocksUseDirectReads                   = "tmdb.rocksdb.use_direct_reads"
	tmdbRocksCreateIfMissing                  = "tmdb.rocksdb.create_if_missing"               // default true
	tmdbRocksIncreaseParallelism              = "tmdb.rocksdb.increase_parallelism"            // default runtime.NumCpu()
	tmdbRocksOptimizeLevelStyleCompaction     = "tmdb.rocksdb.optimize_level_style_compaction" // default 512 * 1024 * 1024

)

type tmdbCreator struct{}

// Create a new tmdb instance with options
func (tmdbCreator) Create(p *properties.Properties) (db ycsb.DB, err error) {
	//	backend := p.GetString(tmdbBackend, "rocksdb")

	return
}

type tmdb struct {
	p  *properties.Properties
	db tm.DB
}

// ycsb.DB interface implementation

func (_ *tmdb) ToSqlDB() *sql.DB {
	return nil
}

func (db *tmdb) Close() (err error) {

	return
}

func (db *tmdb) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *tmdb) CleanupThread(_ context.Context) {}

// Read is the origin database logic
// we have to adapt this interface for inner tmdb fix query to inner logic not SQL like database
func (db *tmdb) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {

	return map[string][]byte{}, nil
}

// Scan also need to adapt for tmdb
func (db *tmdb) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	// tmdb.Iterator() -> Iterator
	// but there is count , in tmdb is end key so how to fix it ?
	return []map[string][]byte{}, nil
}

func (db *tmdb) Update(ctx context.Context, table string, key string, values map[string][]byte) error {

	return nil
}

func (db *tmdb) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {

	return nil
}

func (db *tmdb) Delete(ctx context.Context, table string, key string) error {

	return nil
}

// ycsb.BatchDB interface implementations
// TODO: should convert this functions after the base bench of the tmdb rocksdb

func init() {
	ycsb.RegisterDBCreator("tmdb", tmdbCreator{})
}
