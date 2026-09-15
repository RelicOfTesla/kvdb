module github.com/RelicOfTesla/kvdb/bolt

go 1.25.0

require (
	github.com/RelicOfTesla/kvdb v0.0.0
	go.etcd.io/bbolt v1.5.0
)

require golang.org/x/sys v0.45.0 // indirect

replace github.com/RelicOfTesla/kvdb => ../
