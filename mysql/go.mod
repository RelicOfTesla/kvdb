module github.com/RelicOfTesla/kvdb/mysql

go 1.24.0

require (
	github.com/RelicOfTesla/kvdb v0.0.0
	github.com/RelicOfTesla/kvdb/sqlstore v0.0.0
	github.com/go-sql-driver/mysql v1.10.1
)

require filippo.io/edwards25519 v1.2.0 // indirect

replace (
	github.com/RelicOfTesla/kvdb => ../
	github.com/RelicOfTesla/kvdb/sqlstore => ../sqlstore
)
