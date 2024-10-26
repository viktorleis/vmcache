#ifndef BUFFER_MANAGER_HPP
#define BUFFER_MANAGER_HPP

#include "utils.hpp"
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

namespace std {

    struct PageState {
        atomic<u64> stateAndVersion;

        static const u64 Unlocked = 0;
        static const u64 MaxShared = 252;
        static const u64 Locked = 253;
        static const u64 Marked = 254;
        static const u64 Evicted = 255;

        PageState() {}

        void init() { stateAndVersion.store(sameVersion(0, Evicted), std::memory_order_release); }

        static inline u64 sameVersion(u64 oldStateAndVersion, u64 newState) { return ((oldStateAndVersion<<8)>>8) | newState<<56; }
        static inline u64 nextVersion(u64 oldStateAndVersion, u64 newState) { return (((oldStateAndVersion<<8)>>8)+1) | newState<<56; }

        bool tryLockX(u64 oldStateAndVersion) {
            return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, Locked));
        }

        void unlockX() {
            assert(getState() == Locked);
            stateAndVersion.store(nextVersion(stateAndVersion.load(), Unlocked), std::memory_order_release);
        }

        void unlockXEvicted() {
            assert(getState() == Locked);
            stateAndVersion.store(nextVersion(stateAndVersion.load(), Evicted), std::memory_order_release);
        }

        void downgradeLock() {
            assert(getState() == Locked);
            stateAndVersion.store(nextVersion(stateAndVersion.load(), 1), std::memory_order_release);
        }

        bool tryLockS(u64 oldStateAndVersion) {
            u64 s = getState(oldStateAndVersion);
            if (s<MaxShared)
                return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, s+1));
            if (s==Marked)
                return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, 1));
            return false;
        }

        void unlockS() {
            while (true) {
                u64 oldStateAndVersion = stateAndVersion.load();
                u64 state = getState(oldStateAndVersion);
                assert(state>0 && state<=MaxShared);
                if (stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, state-1)))
                    return;
            }
        }

        bool tryMark(u64 oldStateAndVersion) {
            assert(getState(oldStateAndVersion)==Unlocked);
            return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, Marked));
        }

        static u64 getState(u64 v) { return v >> 56; };
        u64 getState() { return getState(stateAndVersion.load()); }

        void operator=(PageState&) = delete;
    };

    // open addressing hash table used for second chance replacement to keep track of currently-cached pages
    struct ResidentPageSet {
        static const u64 empty = ~0ull;
        static const u64 tombstone = (~0ull)-1;

        struct Entry {
            atomic<u64> pid;
        };

        Entry* ht;
        u64 count;
        u64 mask;
        atomic<u64> clockPos;

        ResidentPageSet(u64 maxCount) : count(next_pow2(maxCount * 1.5)), mask(count - 1), clockPos(0) {
            ht = (Entry*)allocHuge(count * sizeof(Entry));
            memset((void*)ht, 0xFF, count * sizeof(Entry));
        }

        ~ResidentPageSet() {
            munmap(ht, count * sizeof(u64));
        }

        u64 next_pow2(u64 x) {
            return 1<<(64-__builtin_clzl(x-1));
        }

        u64 hash(u64 k) {
            const u64 m = 0xc6a4a7935bd1e995;
            const int r = 47;
            u64 h = 0x8445d61a4e774912 ^ (8*m);
            k *= m;
            k ^= k >> r;
            k *= m;
            h ^= k;
            h *= m;
            h ^= h >> r;
            h *= m;
            h ^= h >> r;
            return h;
        }

        void insert(u64 pid) {
            u64 pos = hash(pid) & mask;
            while (true) {
                u64 curr = ht[pos].pid.load();
                assert(curr != pid);
                if ((curr == empty) || (curr == tombstone))
                    if (ht[pos].pid.compare_exchange_strong(curr, pid))
                        return;

                pos = (pos + 1) & mask;
            }
        }

        bool remove(u64 pid) {
            u64 pos = hash(pid) & mask;
            while (true) {
                u64 curr = ht[pos].pid.load();
                if (curr == empty)
                    return false;

                if (curr == pid)
                    if (ht[pos].pid.compare_exchange_strong(curr, tombstone))
                        return true;

                pos = (pos + 1) & mask;
            }
        }

        template<class Fn>
            void iterateClockBatch(u64 batch, Fn fn) {
                u64 pos, newPos;
                do {
                    pos = clockPos.load();
                    newPos = (pos+batch) % count;
                } while (!clockPos.compare_exchange_strong(pos, newPos));

                for (u64 i=0; i<batch; i++) {
                    u64 curr = ht[pos].pid.load();
                    if ((curr != tombstone) && (curr != empty))
                        fn(curr);
                    pos = (pos + 1) & mask;
                }
            }
    };

    // libaio interface used to write batches of pages
    struct LibaioInterface {
        static const u64 maxIOs = 256;

        int blockfd;
        Page* virtMem;
        io_context_t ctx;
        iocb cb[maxIOs];
        iocb* cbPtr[maxIOs];
        io_event events[maxIOs];

        LibaioInterface(int blockfd, Page* virtMem) : blockfd(blockfd), virtMem(virtMem) {
            memset(&ctx, 0, sizeof(io_context_t));
            int ret = io_setup(maxIOs, &ctx);
            if (ret != 0) {
                std::cerr << "libaio io_setup error: " << ret << " ";
                switch (-ret) {
                    case EAGAIN: std::cerr << "EAGAIN"; break;
                    case EFAULT: std::cerr << "EFAULT"; break;
                    case EINVAL: std::cerr << "EINVAL"; break;
                    case ENOMEM: std::cerr << "ENOMEM"; break;
                    case ENOSYS: std::cerr << "ENOSYS"; break;
                };
                exit(EXIT_FAILURE);
            }
        }

        void writePages(const vector<PID>& pages) {
            assert(pages.size() < maxIOs);
            for (u64 i=0; i<pages.size(); i++) {
                PID pid = pages[i];
                virtMem[pid].dirty = false;
                cbPtr[i] = &cb[i];
                io_prep_pwrite(cb+i, blockfd, &virtMem[pid], pageSize, pageSize*pid);
            }
            int cnt = io_submit(ctx, pages.size(), cbPtr);
            assert(cnt == pages.size());
            cnt = io_getevents(ctx, pages.size(), pages.size(), events, nullptr);
            assert(cnt == pages.size());
        }
    };

    struct BufferManager {
        static const u64 mb = 1024ull * 1024;
        static const u64 gb = 1024ull * 1024 * 1024;
        u64 virtSize;
        u64 physSize;
        u64 virtCount;
        u64 physCount;
        struct exmap_user_interface* exmapInterface[maxWorkerThreads];
        vector<LibaioInterface> libaioInterface;

        bool useExmap;
        int blockfd;
        int exmapfd;

        atomic<u64> physUsedCount;
        ResidentPageSet residentSet;
        atomic<u64> allocCount;

        atomic<u64> readCount;
        atomic<u64> writeCount;

        Page* virtMem;
        PageState* pageState;
        u64 batch;

        PageState& getPageState(PID pid) {
            return pageState[pid];
        }

        BufferManager(params_t* params) : virtSize(params->virtSize*gb), physSize(params->physSize*gb), virtCount(virtSize / pageSize), physCount(physSize / pageSize), useExmap(params->useExmap), residentSet(physCount) {
            assert(virtSize>=physSize);
            const char* path = params->path;
            blockfd = open(path, O_RDWR | O_DIRECT, S_IRWXU);
            if (blockfd == -1) {
                cerr << "cannot open BLOCK device '" << path << "'" << endl;
                exit(EXIT_FAILURE);
            }
            u64 virtAllocSize = virtSize + (1<<16); // we allocate 64KB extra to prevent segfaults during optimistic reads

            if (useExmap) {
                exmapfd = open("/dev/exmap", O_RDWR);
                if (exmapfd < 0) die("open exmap");

                struct exmap_ioctl_setup buffer;
                buffer.fd             = blockfd;
                buffer.max_interfaces = maxWorkerThreads;
                buffer.buffer_size    = physCount;
                buffer.flags          = 0;
                if (ioctl(exmapfd, EXMAP_IOCTL_SETUP, &buffer) < 0)
                    die("ioctl: exmap_setup");

                for (unsigned i=0; i<maxWorkerThreads; i++) {
                    exmapInterface[i] = (struct exmap_user_interface *) mmap(NULL, pageSize, PROT_READ|PROT_WRITE, MAP_SHARED, exmapfd, EXMAP_OFF_INTERFACE(i));
                    if (exmapInterface[i] == MAP_FAILED)
                        die("setup exmapInterface");
                }

                virtMem = (Page*)mmap(NULL, virtAllocSize, PROT_READ|PROT_WRITE, MAP_SHARED, exmapfd, 0);
            } else {
                virtMem = (Page*)mmap(NULL, virtAllocSize, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
                madvise(virtMem, virtAllocSize, MADV_NOHUGEPAGE);
            }

            pageState = (PageState*)allocHuge(virtCount * sizeof(PageState));
            for (u64 i=0; i<virtCount; i++)
                pageState[i].init();
            if (virtMem == MAP_FAILED)
                die("mmap failed");

            libaioInterface.reserve(maxWorkerThreads);
            for (unsigned i=0; i<maxWorkerThreads; i++)
                libaioInterface.emplace_back(LibaioInterface(blockfd, virtMem));

            physUsedCount = 0;
            allocCount = 1; // pid 0 reserved for meta data
            readCount = 0;
            writeCount = 0;
            batch = params->batch;

            cerr << "vmcache " << "blk:" << path << " virtgb:" << virtSize/gb << " physgb:" << physSize/gb << " exmap:" << useExmap << endl;
        }
        ~BufferManager() {}

        Page* fixX(PID pid) {
            PageState& ps = getPageState(pid);
            for (u64 repeatCounter=0; ; repeatCounter++) {
                u64 stateAndVersion = ps.stateAndVersion.load();
                switch (PageState::getState(stateAndVersion)) {
                    case PageState::Evicted: {
                                                 if (ps.tryLockX(stateAndVersion)) {
                                                     handleFault(pid);
                                                     return virtMem + pid;
                                                 }
                                                 break;
                                             }
                    case PageState::Marked: case PageState::Unlocked: {
                                                                          if (ps.tryLockX(stateAndVersion))
                                                                              return virtMem + pid;
                                                                          break;
                                                                      }
                }
                yield(repeatCounter);
            }
        }

        Page* fixS(PID pid) {
            PageState& ps = getPageState(pid);
            for (u64 repeatCounter=0; ; repeatCounter++) {
                u64 stateAndVersion = ps.stateAndVersion;
                switch (PageState::getState(stateAndVersion)) {
                    case PageState::Locked: {
                                                break;
                                            } case PageState::Evicted: {
                                                if (ps.tryLockX(stateAndVersion)) {
                                                    handleFault(pid);
                                                    ps.unlockX();
                                                }
                                                break;
                                            }
                    default: {
                                 if (ps.tryLockS(stateAndVersion))
                                     return virtMem + pid;
                             }
                }
                yield(repeatCounter);
            }
        }

        void unfixS(PID pid) {
            getPageState(pid).unlockS();
        }

        void unfixX(PID pid) {
            getPageState(pid).unlockX();
        }

        bool isValidPtr(void* page) { return (page >= virtMem) && (page < (virtMem + virtSize + 16)); }
        PID toPID(void* page) { return reinterpret_cast<Page*>(page) - virtMem; }
        Page* toPtr(PID pid) { return virtMem + pid; }

        void ensureFreePages() {
            if (physUsedCount >= physCount*0.95)
                evict();
        }

        // allocated new page and fix it
        Page* allocPage() {
            physUsedCount++;
            ensureFreePages();
            u64 pid = allocCount++;
            if (pid >= virtCount) {
                cerr << "VIRTGB is too low" << endl;
                exit(EXIT_FAILURE);
            }
            u64 stateAndVersion = getPageState(pid).stateAndVersion;
            bool succ = getPageState(pid).tryLockX(stateAndVersion);
            assert(succ);
            residentSet.insert(pid);

            if (useExmap) {
                exmapInterface[workerThreadId]->iov[0].page = pid;
                exmapInterface[workerThreadId]->iov[0].len = 1;
                while (exmapAction(exmapfd, EXMAP_OP_ALLOC, 1) < 0) {
                    cerr << "allocPage errno: " << errno << " pid: " << pid << " workerId: " << workerThreadId << endl;
                    ensureFreePages();
                }
            }
            virtMem[pid].dirty = true;

            return virtMem + pid;
        }

        void handleFault(PID pid) {
            physUsedCount++;
            ensureFreePages();
            readPage(pid);
            residentSet.insert(pid);
        }

        void readPage(PID pid) {
            if (useExmap) {
                for (u64 repeatCounter=0; ; repeatCounter++) {
                    int ret = pread(exmapfd, virtMem+pid, pageSize, workerThreadId);
                    if (ret == pageSize) {
                        assert(ret == pageSize);
                        readCount++;
                        return;
                    }
                    cerr << "readPage errno: " << errno << " pid: " << pid << " workerId: " << workerThreadId << endl;
                    ensureFreePages();
                }
            } else {
                int ret = pread(blockfd, virtMem+pid, pageSize, pid*pageSize);
                assert(ret==pageSize);
                readCount++;
            }
        }

        void evict() {
            vector<PID> toEvict;
            toEvict.reserve(batch);
            vector<PID> toWrite;
            toWrite.reserve(batch);

            // 0. find candidates, lock dirty ones in shared mode
            while (toEvict.size()+toWrite.size() < batch) {
                residentSet.iterateClockBatch(batch, [&](PID pid) {
                        PageState& ps = getPageState(pid);
                        u64 v = ps.stateAndVersion;
                        switch (PageState::getState(v)) {
                        case PageState::Marked:
                        if (virtMem[pid].dirty) {
                        if (ps.tryLockS(v))
                        toWrite.push_back(pid);
                        } else {
                        toEvict.push_back(pid);
                        }
                        break;
                        case PageState::Unlocked:
                        ps.tryMark(v);
                        break;
                        default:
                        break; // skip
                        };
                        });
            }

            // 1. write dirty pages
            libaioInterface[workerThreadId].writePages(toWrite);
            writeCount += toWrite.size();

            // 2. try to lock clean page candidates
            toEvict.erase(std::remove_if(toEvict.begin(), toEvict.end(), [&](PID pid) {
                        PageState& ps = getPageState(pid);
                        u64 v = ps.stateAndVersion;
                        return (PageState::getState(v) != PageState::Marked) || !ps.tryLockX(v);
                        }), toEvict.end());

            // 3. try to upgrade lock for dirty page candidates
            for (auto& pid : toWrite) {
                PageState& ps = getPageState(pid);
                u64 v = ps.stateAndVersion;
                if ((PageState::getState(v) == 1) && ps.stateAndVersion.compare_exchange_weak(v, PageState::sameVersion(v, PageState::Locked)))
                    toEvict.push_back(pid);
                else
                    ps.unlockS();
            }

            // 4. remove from page table
            if (useExmap) {
                for (u64 i=0; i<toEvict.size(); i++) {
                    exmapInterface[workerThreadId]->iov[i].page = toEvict[i];
                    exmapInterface[workerThreadId]->iov[i].len = 1;
                }
                if (exmapAction(exmapfd, EXMAP_OP_FREE, toEvict.size()) < 0)
                    die("ioctl: EXMAP_OP_FREE");
            } else {
                for (u64& pid : toEvict)
                    madvise(virtMem + pid, pageSize, MADV_DONTNEED);
            }

            // 5. remove from hash table and unlock
            for (u64& pid : toEvict) {
                bool succ = residentSet.remove(pid);
                assert(succ);
                getPageState(pid).unlockXEvicted();
            }

            physUsedCount -= toEvict.size();
        }
    };
}

#endif

