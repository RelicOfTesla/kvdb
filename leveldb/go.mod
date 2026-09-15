module github.com/RelicOfTesla/kvdb/leveldb

go 1.19

require (
	github.com/RelicOfTesla/kvdb v0.0.0
	github.com/syndtr/goleveldb v1.0.0
)

require github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect

replace github.com/RelicOfTesla/kvdb => ../
