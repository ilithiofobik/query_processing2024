#pragma once

#include <atomic>
#include <cstdint>

#include "qp/common/memory.hpp"
#include "qp/common/util.hpp"

using namespace qp;

template <typename K, typename V>
struct Entry {
    K key;
    V value;
    Entry *next;

    Entry(K k, V v) {
        key = k;
        value = v;
        next = NULL;
    }
};

typedef uint64_t TaggedPointer;

const uint64_t tagMask = 0xffff000000000000;
const uint64_t untagMask = 0x0000ffffffffffff;

template <typename K, typename V>
struct Hashtable {
    uint64_t htSize;
    uint64_t mask;
    std::atomic<Entry<K, V> *> *ht;

    Hashtable(uint64_t size) {
        htSize = 1ull << ((8 * sizeof(uint64_t)) -
                          __builtin_clzl(size));  // next power of two
        mask = htSize - 1;
        ht = reinterpret_cast<std::atomic<Entry<K, V> *> *>(
            qp::mem::alloc(sizeof(uint64_t) * htSize));

        for (uint64_t slot = 0; slot < size; slot++) {
            ht[slot].store(NULL);
        }
    }

    ~Hashtable() { qp::mem::dealloc(ht, htSize); }

    struct equal_range_iterator {
        K key;
        Entry<K, V> *entry;
    };

    std::pair<equal_range_iterator, equal_range_iterator> equal_range(K key) {
        uint64_t hash = hashKey(key);
        uint64_t slot = hash & mask;
        Entry<K, V> *beginIter = NULL;
        Entry<K, V> *endIter = NULL;

#ifdef VARIANT_tagged
        uint64_t firstAddress = (uint64_t)ht[slot].load();
        if (firstAddress & addTag(hash)) {
            Entry<K, V> *it = removeTag(firstAddress);

            while (it != NULL) {
                if (it->key == key) {
                    if (beginIter == NULL) {
                        beginIter = it;
                    }
                    endIter = it->next;
                }
                it = it->next;
            }
        }
#else
        Entry<K, V> *it = ht[slot].load();

        while (it != NULL) {
            if (it->key == key) {
                if (beginIter == NULL) {
                    beginIter = it;
                }
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
        Entry<K, V> *oldEnt = ht[slot].load();
        Entry<K, V> *newEnt = newEntry;
        do {
            oldEnt = ht[slot].load();
#ifdef VARIANT_tagged
            uint64_t oltEntInt = (uint64_t)oldEnt;
            uint64_t newEntInt = (uint64_t)newEnt;

            newEntry->next = removeTag(oltEntInt);
            newEnt = (Entry<K, V> *)(newEntInt | (oltEntInt & tagMask) |
                                     addTag(hashKey(newEntry->key) & mask));
#else
            newEntry->next = oldEnt;
            newEnt = newEntry;
#endif

        } while (!ht[slot].compare_exchange_weak(oldEnt, newEnt));
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
        uint64_t untagged = entryPtr & untagMask;
        return (Entry<K, V> *)untagged;
    }

    inline uint64_t addTag(uint64_t hash) {
        uint64_t tagPow = 48 + (hash >> 60);  // first 4 bits, values 0 - 15
        return 1ull << tagPow;  // returns 1-bit in first 16 bits, rest is 0
    }
};
