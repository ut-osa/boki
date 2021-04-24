module cs.utexas.edu/zjia/faas/slib

go 1.14

require (
	cs.utexas.edu/zjia/faas v0.0.0
	github.com/Jeffail/gabs/v2 v2.6.0
	github.com/go-redis/redis/v8 v8.8.2 // indirect
	github.com/golang/snappy v0.0.2
)

replace cs.utexas.edu/zjia/faas => ../worker/golang
