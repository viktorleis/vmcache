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
#COMMON += -O3 -DNDEBUG -DCONF_debug_memory=0 -DOSV
COMMON += -g -DOSV -DCONF_debug_memory=0
COMMON += -fnon-call-exceptions -fasynchronous-unwind-tables

CXXFLAGS = -std=c++20 $(COMMON) -shared -fPIC
LDFLAGS = -shared -fPIC

osv-vmcache: vmcache_modularized.cpp tpcc/*
	$(CXX) $(CXXFLAGS) $(local-includes) $(INCLUDES)  vmcache_modularized.cpp -o vmcache


all: linux-vmcache

module: osv-vmcache

clean:
	        rm vmcache
