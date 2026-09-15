module github.com/RelicOfTesla/kvdb/example

go 1.25.0

require (
	github.com/RelicOfTesla/kvdb v0.0.0
	github.com/RelicOfTesla/kvdb/all v0.0.0
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/RelicOfTesla/kvdb/bolt v0.0.0 // indirect
	github.com/RelicOfTesla/kvdb/jsonl v0.0.0 // indirect
	github.com/RelicOfTesla/kvdb/mem v0.0.0 // indirect
	github.com/RelicOfTesla/kvdb/mysql v0.0.0 // indirect
	github.com/RelicOfTesla/kvdb/pg v0.0.0 // indirect
	github.com/RelicOfTesla/kvdb/redis v0.0.0 // indirect
	github.com/RelicOfTesla/kvdb/sqlite v0.0.0 // indirect
	github.com/RelicOfTesla/kvdb/sqlstore v0.0.0 // indirect
	github.com/RelicOfTesla/kvdb/ssdb v0.0.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-sql-driver/mysql v1.10.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.11.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/redis/go-redis/v9 v9.22.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	go.etcd.io/bbolt v1.5.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.29.0 // indirect
	modernc.org/libc v1.75.6 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.1 // indirect
	modernc.org/sqlite v1.58.0 // indirect
)

replace (
	github.com/RelicOfTesla/kvdb => ../
	github.com/RelicOfTesla/kvdb/all => ../all
	github.com/RelicOfTesla/kvdb/bolt => ../bolt
	github.com/RelicOfTesla/kvdb/jsonl => ../jsonl
	github.com/RelicOfTesla/kvdb/mem => ../mem
	github.com/RelicOfTesla/kvdb/mysql => ../mysql
	github.com/RelicOfTesla/kvdb/pg => ../pg
	github.com/RelicOfTesla/kvdb/redis => ../redis
	github.com/RelicOfTesla/kvdb/sqlite => ../sqlite
	github.com/RelicOfTesla/kvdb/sqlstore => ../sqlstore
	github.com/RelicOfTesla/kvdb/ssdb => ../ssdb
)
