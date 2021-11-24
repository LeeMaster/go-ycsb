package tmdb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
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
	// backend := p.GetString(tmdbBackend, "rocksdb")
	// TODO: should add another backend type support but current we just
	// need the simple rocksdb bench
	name := p.GetString(tmdbName, "benchmark")
	dir := p.GetString(tmdbDir, "/tmp/benchmark")

	constructViperOptions(p)

	inner := tm.NewDB(name, tm.RocksDBBackend, dir)

	db = &tmdb{
		p:  p,
		db: inner,
		r:  util.NewRowCodec(p),
	}

	return
}

// constructViperOptions will construct the inner tmdb viper logic
// to construct a new rocksdb instance
func constructViperOptions(p *properties.Properties) {
	// TODO: should use this create the rocksdb.opts flag and the configuration use the K=V , splitor logic format
}

type tmdb struct {
	p  *properties.Properties
	db tm.DB

	r       *util.RowCodec
	bufPool *util.BufPool
}

// ycsb.DB interface implementation

func (_ *tmdb) ToSqlDB() *sql.DB {
	return nil
}

func (db *tmdb) Close() (err error) {
	err = db.db.Close()
	return
}

func (db *tmdb) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *tmdb) CleanupThread(_ context.Context) {}

func (db *tmdb) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

// Read is the origin database logic
// we have to adapt this interface for inner tmdb fix query to inner logic not SQL like database
func (db *tmdb) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	value, err := db.db.Get(db.getRowKey(table, key))
	if err != nil {
		return nil, err
	}
	return db.r.Decode(value, fields)
}

// Scan also need to adapt for tmdb
func (db *tmdb) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	it, err := db.db.Iterator(db.getRowKey(table, startKey), nil)
	if err != nil {
		return nil, err
	}
	i := 0
	for ; it.Valid() && i < count; it.Next() {
		value := it.Value()
		m, err := db.r.Decode(value, fields)
		if err != nil {
			return nil, err
		}
		res[i] = m
		i++
	}

	if err := it.Error(); err != nil {
		return nil, err
	}

	return res, nil
}

func (db *tmdb) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	m, err := db.Read(ctx, table, key, nil)
	if err != nil {
		return err
	}

	for field, value := range values {
		m[field] = value
	}

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	rowData, err := db.r.Encode(buf.Bytes(), m)
	if err != nil {
		return err
	}

	rowKey := db.getRowKey(table, key)

	return db.db.Set(rowKey, rowData)
}

func (db *tmdb) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	rowKey := db.getRowKey(table, key)

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	rowData, err := db.r.Encode(buf.Bytes(), values)
	if err != nil {
		return err
	}

	return db.db.Set(rowKey, rowData)
}

func (db *tmdb) Delete(ctx context.Context, table string, key string) error {
	rowKey := db.getRowKey(table, key)
	return db.db.Delete(rowKey)
}

// ycsb.BatchDB interface implementations
// TODO: should convert this functions after the base bench of the tmdb rocksdb

func init() {
	ycsb.RegisterDBCreator("tmdb", tmdbCreator{})
}
