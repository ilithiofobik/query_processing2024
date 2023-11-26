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

        bool operator==(const equal_range_iterator &a) const {
            return (key == a.key && entry == a.entry);
        }
        bool operator!=(const equal_range_iterator &a) const {
            return (entry != a.entry || key != a.key);
        }
        equal_range_iterator &operator++() {
            do {
                entry = entry->next;
            } while (entry != NULL && entry->key != key);

            return *this;
        }
    };

    std::pair<equal_range_iterator, equal_range_iterator> equal_range(K key) {
        uint64_t hash = hashKey(key);
        uint64_t slot = hash & mask;
        Entry<K, V> *beginIter = NULL;

#ifdef VARIANT_tagged
        uint64_t firstAddress = (uint64_t)ht[slot].load();
        if (firstAddress & addTag(hash)) {
            Entry<K, V> *it = removeTag(firstAddress);

            while (it != NULL) {
                if (it->key == key) {
                    beginIter = it;
                    break;
                }
                it = it->next;
            }
        }
#else
        Entry<K, V> *it = ht[slot].load();

        while (it != NULL) {
            if (it->key == key) {
                beginIter = it;
                break;
            }
            it = it->next;
        }
#endif

        equal_range_iterator begin = {key, beginIter};
        equal_range_iterator end = {key, NULL};
        return std::make_pair(begin, end);
    }

    void insert(Entry<K, V> *newEntry) {
        uint64_t hash = hashKey(newEntry->key);
        uint64_t slot = hash & mask;
        Entry<K, V> *oldEnt = ht[slot].load();
        Entry<K, V> *newEnt = newEntry;
        do {
            oldEnt = ht[slot].load();
#ifdef VARIANT_tagged
            uint64_t oltEntInt = (uint64_t)oldEnt;
            uint64_t newEntInt = (uint64_t)newEnt;

            newEntry->next = removeTag(oltEntInt);
            newEnt = (Entry<K, V> *)(newEntInt | (oltEntInt & tagMask) |
                                     addTag(hash));
#else
            newEntry->next = oldEnt;
            newEnt = newEntry;
#endif

        } while (!ht[slot].compare_exchange_weak(oldEnt, newEnt));
    }

   private:
    inline Entry<K, V> *removeTag(uint64_t entryPtr) {
        uint64_t untagged = entryPtr & untagMask;
        return (Entry<K, V> *)untagged;
    }

    inline uint64_t addTag(uint64_t hash) {
        uint64_t tagPow = 48 + (hash >> 60);  // first 4 bits, values 0 - 15
        return 1ull << tagPow;  // returns 1-bit in first 16 bits, rest is 0
    }
};
