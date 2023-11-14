#pragma once

#include <cstdint>

#include "qp/common/memory.hpp"
#include "qp/common/util.hpp"

using namespace qp;

template <typename K, typename V>
struct Entry {
    K key;
    V value;
    Entry *next;
};

typedef uint64_t TaggedPointer;

const uint64_t tagMask = 0xffff000000000000;
const uint64_t untagMask = 0x0000ffffffffffff;

template <typename K, typename V>
struct Hashtable {
    uint64_t htSize;
    uint64_t mask;
    Entry<K, V> **ht;

    Hashtable(uint64_t size) {
        htSize = 1ull << ((8 * sizeof(uint64_t)) -
                          __builtin_clzl(size));  // next power of two
        mask = htSize - 1;
        ht = reinterpret_cast<Entry<K, V> **>(
            qp::mem::alloc(sizeof(uint64_t) * htSize));
    }

    ~Hashtable() { qp::mem::dealloc(ht, htSize); }

    struct equal_range_iterator {
        K key;
        Entry<K, V> *entry;
    };

    std::pair<equal_range_iterator, equal_range_iterator> equal_range(K key) {
        uint64_t hash = hashKey(key);
        uint64_t slot = hash & mask;
        uint64_t firstAddress = ht[slot];
        Entry<K, V> *beginIter = nullptr;
        Entry<K, V> *endIter = nullptr;

#ifdef VARIANT_tagged
        if (firstAddress & addTag(hash)) {
            Entry<K, V> *it = removeTag(firstAddress);

            while (it) {
                if (it->key == key) {
                    beginIter = it;
                    endIter = it->next;
                    break;
                }
                it = it->next;
            }

            while (it) {
                if (it->key == key) {
                    endIter = it->next;
                }
                it = it->next;
            }
        }
#else
        Entry<K, V> *it = firstAddress;

        while (it) {
            if (it->key == key) {
                beginIter = it;
                endIter = it->next;
                break;
            }
            it = it->next;
        }

        while (it) {
            if (it->key == key) {
                endIter = it->next;
            }
            it = it->next;
        }
#endif

        equal_range_iterator begin = {key, beginIter};
        equal_range_iterator end = {key, endIter};
        return std::make_pair(begin, end);
    }

    void insert(Entry<K, V> *newEntry) {
        uint64_t slot = hashKey(newEntry->key) & mask;
        auto oldEnt = ht[slot];
        auto newEnt = newEntry;
        do {
            oldEnt = ht[slot];
#ifdef VARIANT_tagged
            newEntry->next = removeTag(oldEnt);
            newEnt = newEntry | (oldEnt & tagMask) | addTag(newEntry->hash);
#else
            newEntry->next = oldEnt;
            newEnt = newEntry;
#endif

        } while (!std::atomic_compare_exchange_weak_explicit(ht[slot], oldEnt,
                                                             newEnt));
    }

   private:
    //---------------------------------------------------------------------------
    template <typename T, typename F, unsigned I = 0, typename... Args>
    constexpr inline T __fold_tuple(const std::tuple<Args...> &tuple,
                                    T acc_or_init, const F &fn) {
        if constexpr (I == sizeof...(Args)) {
            return acc_or_init;
        } else {
            return __fold_tuple<T, F, I + 1, Args...>(
                tuple, fn(acc_or_init, std::get<I>(tuple)), fn);
        }
    }
    //---------------------------------------------------------------------------
    // extends hashKey function from Types.hpp to support tuples
    template <typename... Args>
    inline uint64_t hashKey(const std::tuple<Args...> &args) {
        return __fold_tuple(
            args, 0ul, [](const uint64_t acc, const auto &val) -> uint64_t {
                // adapted from boost::hash_combine
                return acc ^ (MurmurHash64A(val) + 0x517cc1b727220a95ul +
                              (acc << 6) + (acc >> 2));
                // bad if keys are the same!
                // return hashKey(val) ^ acc;
            });
    }

    inline Entry<K, V> *removeTag(uint64_t entryPtr) {
        auto untagged = entryPtr & untagMask;
        return untagged;
    }

    inline uint64_t addTag(uint64_t hash) {
        uint64_t tagPow = hash >> 60;  // first 4 bits, values 0 - 15
        return 1ull << (48 +
                        tagPow);  // returns 1-bit in first 16 bits, rest is 0
    }
};
