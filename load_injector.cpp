#include <atomic>
#include <algorithm>
#include <cassert>
#include <csignal>
#include <exception>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <mutex>
#include <numeric>
#include <set>
#include <thread>
#include <vector>
#include <span>

#include <errno.h>
#include <libaio.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <immintrin.h>

#include "exmap.h"

__thread uint16_t workerThreadId = 0;
__thread int32_t tpcchistorycounter = 0;
#include "tpcc/TPCCWorkload.hpp"

#include "utils.hpp"
#include "adapter.hpp"
#include "btree.hpp"

using namespace std;

int main(int argc, char** argv) {
    bool useExmap = envOr("EXMAP", 0);
    if (useExmap) {
        struct sigaction action;
        action.sa_flags = SA_SIGINFO;
        action.sa_sigaction = handleSEGFAULT;
        if (sigaction(SIGSEGV, &action, NULL) == -1) {
            perror("sigusr: sigaction");
            exit(1);
        }
    }

    unsigned nthreads = envOr("THREADS", 1);
    u64 n = envOr("DATASIZE", 10);
    u64 runForSec = envOr("RUNFOR", 30);
    bool isRndread = envOr("RNDREAD", 0);
    params_t params;
    params.virtSize = envOr("VIRTGB", 16);
    params.physSize = envOr("PHYSGB", 4);
    params.batch = envOr("BATCH", 64);
    params.useExmap = useExmap;
    params.path = getenv("BLOCK") ? getenv("BLOCK"): "/tmp/bm";


    u64 statDiff = 1e8;
    atomic<u64> txProgress(0);
    atomic<bool> keepRunning(true);
    auto systemName = useExmap ? "exmap" : "vmcache";

    auto statFn = [&]() {
        cout << "ts,tx,rmb,wmb,system,threads,datasize,workload,batch" << endl;
        u64 cnt = 0;
        for (uint64_t i=0; i<runForSec; i++) {
            sleep(1);
            float rmb = (bm->readCount.exchange(0)*pageSize)/(1024.0*1024);
            float wmb = (bm->writeCount.exchange(0)*pageSize)/(1024.0*1024);
            u64 prog = txProgress.exchange(0);
            cout << cnt++ << "," << prog << "," << rmb << "," << wmb << "," << systemName << "," << nthreads << "," << n << "," << (isRndread?"rndread":"tpcc") << "," << bm->batch << endl;
        }
        keepRunning = false;
    };

    if (isRndread) {
        BTree* bt = new BTree(&params);
        bt->splitOrdered = true;

        {
            // insert
            parallel_for(0, n, nthreads, [&](uint64_t worker, uint64_t begin, uint64_t end) {
                    workerThreadId = worker;
                    array<u8, 120> payload;
                    for (u64 i=begin; i<end; i++) {
                    union { u64 v1; u8 k1[sizeof(u64)]; };
                    v1 = __builtin_bswap64(i);
                    memcpy(payload.data(), k1, sizeof(u64));
                    bt->insert({k1, sizeof(KeyType)}, payload);
                    }
                    });
        }
        cerr << "space: " << (bm->allocCount.load()*pageSize)/(float)bm->gb << " GB " << endl;

        bm->readCount = 0;
        bm->writeCount = 0;
        thread statThread(statFn);

        parallel_for(0, nthreads, nthreads, [&](uint64_t worker, uint64_t begin, uint64_t end) {
                workerThreadId = worker;
                u64 cnt = 0;
                u64 start = rdtsc();
                while (keepRunning.load()) {
                union { u64 v1; u8 k1[sizeof(u64)]; };
                v1 = __builtin_bswap64(RandomGenerator::getRand<u64>(0, n));

                array<u8, 120> payload;
                bool succ = bt->lookup({k1, sizeof(u64)}, [&](span<u8> p) {
                        memcpy(payload.data(), p.data(), p.size());
                        });
                assert(succ);
                assert(memcmp(k1, payload.data(), sizeof(u64))==0);

                cnt++;
                u64 stop = rdtsc();
                if ((stop-start) > statDiff) {
                txProgress += cnt;
                start = stop;
                cnt = 0;
                }
                }
                txProgress += cnt;
        });

        statThread.join();
        return 0;
    }

    // TPC-C
    Integer warehouseCount = n;

    vmcacheAdapter<warehouse_t> warehouse(&params);
    vmcacheAdapter<district_t> district(&params);
    vmcacheAdapter<customer_t> customer(&params);
    vmcacheAdapter<customer_wdl_t> customerwdl(&params);
    vmcacheAdapter<history_t> history(&params);
    vmcacheAdapter<neworder_t> neworder(&params);
    vmcacheAdapter<order_t> order(&params);
    vmcacheAdapter<order_wdc_t> order_wdc(&params);
    vmcacheAdapter<orderline_t> orderline(&params);
    vmcacheAdapter<item_t> item(&params);
    vmcacheAdapter<stock_t> stock(&params);

    TPCCWorkload<vmcacheAdapter> tpcc(warehouse, district, customer, customerwdl, history, neworder, order, order_wdc, orderline, item, stock, true, warehouseCount, true);

    {
        tpcc.loadItem();
        tpcc.loadWarehouse();

        parallel_for(1, warehouseCount+1, nthreads, [&](uint64_t worker, uint64_t begin, uint64_t end) {
                workerThreadId = worker;
                for (Integer w_id=begin; w_id<end; w_id++) {
                tpcc.loadStock(w_id);
                tpcc.loadDistrinct(w_id);
                for (Integer d_id = 1; d_id <= 10; d_id++) {
                tpcc.loadCustomer(w_id, d_id);
                tpcc.loadOrders(w_id, d_id);
                }
                }
                });
    }
    cerr << "space: " << (bm->allocCount.load()*pageSize)/(float)bm->gb << " GB " << endl;

    bm->readCount = 0;
    bm->writeCount = 0;
    thread statThread(statFn);

    parallel_for(0, nthreads, nthreads, [&](uint64_t worker, uint64_t begin, uint64_t end) {
            workerThreadId = worker;
            u64 cnt = 0;
            u64 start = rdtsc();
            while (keepRunning.load()) {
            int w_id = tpcc.urand(1, warehouseCount); // wh crossing
            tpcc.tx(w_id);
            cnt++;
            u64 stop = rdtsc();
            if ((stop-start) > statDiff) {
            txProgress += cnt;
            start = stop;
            cnt = 0;
            }
            }
            txProgress += cnt;
            });

    statThread.join();
    cerr << "space: " << (bm->allocCount.load()*pageSize)/(float)bm->gb << " GB " << endl;

    return 0;
}
