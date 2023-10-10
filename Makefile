all: vmcache test

vmcache: vmcache.cpp tpcc/*pp
	g++ -std=c++20 -g -fnon-call-exceptions -fasynchronous-unwind-tables vmcache.cpp -o vmcache -laio

test: test_mmap_osv.c
	gcc -g test_mmap_osv.c -o test

clean:
	rm vmcache
