linux-vmcache: vmcache.cpp tpcc/*pp
	g++ -std=c++20 -O3 -DLINUX -fnon-call-exceptions -fasynchronous-unwind-tables vmcache.cpp -o vmcache -laio

osv-vmcache: 
src= ../..
include $(src)/modules/common.gmk

detect_arch=$(shell echo $(1) | $(CC) -E -xc- | tail -n 1)

ARCH = x64
MODE = release

local-includes = -I.
autodepend = -MD -MT $@ -MP

COMMON += -D __BSD_VISIBLE=1
COMMON += -include $(src)/compiler/include/intrinsics.hh
COMMON += -O3 -DNDEBUG -DCONF_debug_memory=0 -DOSV
#COMMON += -g -DOSV -DCONF_debug_memory=0
COMMON += -fnon-call-exceptions -fasynchronous-unwind-tables

CXXFLAGS = -std=c++20 $(COMMON) -shared -fPIC
LDFLAGS = -shared -fPIC

osv-vmcache: vmcache.cpp tpcc/*
	$(CXX) $(CXXFLAGS) $(local-includes) $(INCLUDES)  vmcache.cpp -o vmcache


all: linux-vmcache

module: osv-vmcache

load_injector: load_injector.cpp tpcc/*pp *.hpp
	g++ -DNDEBUG -O3 -std=c++20 -g -fnon-call-exceptions -fasynchronous-unwind-tables load_injector.cpp -o load_injector -laio

all: vmcache load_injector

clean:
	        rm vmcache
