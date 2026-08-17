module github.com/ant-caor/nimbus/examples/redisbus

go 1.25.0

require (
	github.com/ant-caor/nimbus v0.0.0
	github.com/redis/rueidis v1.0.77
)

require (
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/ant-caor/nimbus => ../..
