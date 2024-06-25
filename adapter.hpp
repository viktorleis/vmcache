#ifndef ADAPTER_HPP
#define ADAPTER_HPP

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
#include "utils.hpp"
#include "btree.hpp"

namespace std{

    template <class Record>
        struct vmcacheAdapter
        {
            BTree* tree;

            vmcacheAdapter(params_t* params) {
                tree = new BTree(params);
            }

            public:
            void scan(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& found_record_cb, std::function<void()> reset_if_scan_failed_cb) {
                u8 k[Record::maxFoldLength()];
                u16 l = Record::foldKey(k, key);
                u8 kk[Record::maxFoldLength()];
                tree->scanAsc({k, l}, [&](BTreeNode& node, unsigned slot) {
                        memcpy(kk, node.getPrefix(), node.prefixLen);
                        memcpy(kk+node.prefixLen, node.getKey(slot), node.slot[slot].keyLen);
                        typename Record::Key typedKey;
                        Record::unfoldKey(kk, typedKey);
                        return found_record_cb(typedKey, *reinterpret_cast<const Record*>(node.getPayload(slot).data()));
                        });
            }
            // -------------------------------------------------------------------------------------
            void scanDesc(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& found_record_cb, std::function<void()> reset_if_scan_failed_cb) {
                u8 k[Record::maxFoldLength()];
                u16 l = Record::foldKey(k, key);
                u8 kk[Record::maxFoldLength()];
                bool first = true;
                tree->scanDesc({k, l}, [&](BTreeNode& node, unsigned slot, bool exactMatch) {
                        if (first) { // XXX: hack
                        first = false;
                        if (!exactMatch)
                        return true;
                        }
                        memcpy(kk, node.getPrefix(), node.prefixLen);
                        memcpy(kk+node.prefixLen, node.getKey(slot), node.slot[slot].keyLen);
                        typename Record::Key typedKey;
                        Record::unfoldKey(kk, typedKey);
                        return found_record_cb(typedKey, *reinterpret_cast<const Record*>(node.getPayload(slot).data()));
                        });
            }
            // -------------------------------------------------------------------------------------
            void insert(const typename Record::Key& key, const Record& record) {
                u8 k[Record::maxFoldLength()];
                u16 l = Record::foldKey(k, key);
                tree->insert({k, l}, {(u8*)(&record), sizeof(Record)});
            }
            // -------------------------------------------------------------------------------------
            template<class Fn>
                void lookup1(const typename Record::Key& key, Fn fn) {
                    u8 k[Record::maxFoldLength()];
                    u16 l = Record::foldKey(k, key);
                    bool succ = tree->lookup({k, l}, [&](span<u8> payload) {
                            fn(*reinterpret_cast<const Record*>(payload.data()));
                            });
                    assert(succ);
                }
            // -------------------------------------------------------------------------------------
            template<class Fn>
                void update1(const typename Record::Key& key, Fn fn) {
                    u8 k[Record::maxFoldLength()];
                    u16 l = Record::foldKey(k, key);
                    tree->updateInPlace({k, l}, [&](span<u8> payload) {
                            fn(*reinterpret_cast<Record*>(payload.data()));
                            });
                }
            // -------------------------------------------------------------------------------------
            // Returns false if the record was not found
            bool erase(const typename Record::Key& key) {
                u8 k[Record::maxFoldLength()];
                u16 l = Record::foldKey(k, key);
                return tree->remove({k, l});
            }
            // -------------------------------------------------------------------------------------
            template <class Field>
                Field lookupField(const typename Record::Key& key, Field Record::*f) {
                    Field value;
                    lookup1(key, [&](const Record& r) { value = r.*f; });
                    return value;
                }

            u64 count() {
                u64 cnt = 0;
                tree->scanAsc({(u8*)nullptr, 0}, [&](BTreeNode& node, unsigned slot) { cnt++; return true; } );
                return cnt;
            }

            u64 countw(Integer w_id) {
                u8 k[sizeof(Integer)];
                fold(k, w_id);
                u64 cnt = 0;
                u8 kk[Record::maxFoldLength()];
                tree->scanAsc({k, sizeof(Integer)}, [&](BTreeNode& node, unsigned slot) {
                        memcpy(kk, node.getPrefix(), node.prefixLen);
                        memcpy(kk+node.prefixLen, node.getKey(slot), node.slot[slot].keyLen);
                        if (memcmp(k, kk, sizeof(Integer))!=0)
                        return false;
                        cnt++;
                        return true;
                        });
                return cnt;
            }
        };
}
#endif
