module github.com/RelicOfTesla/kvdb/jsonl

go 1.18

require (
	github.com/RelicOfTesla/kvdb v0.0.0
	github.com/RelicOfTesla/kvdb/mem v0.0.0
)

replace (
	github.com/RelicOfTesla/kvdb => ../
	github.com/RelicOfTesla/kvdb/mem => ../mem
)
