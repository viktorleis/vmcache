vmcache: vmcache.cpp tpcc/*pp
	g++ -DNDEBUG -O3 -std=c++20 -g -fnon-call-exceptions -fasynchronous-unwind-tables vmcache.cpp -o vmcache -laio

load_injector: load_injector.cpp tpcc/*pp *.hpp
	g++ -DNDEBUG -O3 -std=c++20 -g -fnon-call-exceptions -fasynchronous-unwind-tables load_injector.cpp -o load_injector -laio

all: vmcache load_injector

clean:
	rm vmcache
