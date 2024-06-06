vmcache: vmcache.cpp tpcc/*pp
	g++ -DNDEBUG -O3 -std=c++20 -g -fnon-call-exceptions -fasynchronous-unwind-tables vmcache.cpp -o vmcache -laio

vmcache_dpdk: vmcache.cpp dpdk_memcmp.hh tpcc/*pp
	g++ -DNDEBUG -O3 -DDPDK_MEMCMP -mavx -mavx2 -std=c++20 -g -fnon-call-exceptions -fasynchronous-unwind-tables vmcache.cpp -o vmcache_dpdk -laio

all: vmcache vmcache_dpdk

clean:
	rm vmcache
