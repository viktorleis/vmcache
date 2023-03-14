vmcache: vmcache.cpp tpcc/*pp
	g++ -DNDEBUG -O3 -std=c++20 -g -fnon-call-exceptions -fasynchronous-unwind-tables vmcache.cpp -o vmcache -ltbb -laio

clean:
	rm vmcache
