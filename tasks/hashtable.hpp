#pragma once
#include <cstdint>

using namespace qp;

template<typename K, typename V>
struct Entry {
   K key;
   V value;
   Entry* next;
};

typedef uint64_t TaggedPointer;

template<typename K, typename V>
struct Hashtable {
   Hashtable(uint64_t size) { }
   ~Hashtable() { }

   struct equal_range_iterator {
   };

   std::pair<equal_range_iterator, equal_range_iterator> equal_range(K key) {
#ifdef VARIANT_tagged
#else
#endif
   }

   void insert(Entry<K, V>* newEntry) {
#ifdef VARIANT_tagged
#else
#endif
   }

private:
   //---------------------------------------------------------------------------
   template <typename T, typename F, unsigned I = 0, typename... Args>
      constexpr inline T __fold_tuple(const std::tuple<Args...> &tuple, T acc_or_init, const F &fn)
      {
         if constexpr (I == sizeof...(Args))
         {
            return acc_or_init;
         }
         else
         {
            return __fold_tuple<T, F, I + 1, Args...>(tuple, fn(acc_or_init, std::get<I>(tuple)), fn);
         }
      }
   //---------------------------------------------------------------------------
   // extends hashKey function from Types.hpp to support tuples
   template <typename... Args>
      inline uint64_t hashKey(const std::tuple<Args...> &args)
      {
         return __fold_tuple(args, 0ul,
               [](const uint64_t acc, const auto &val) -> uint64_t
               {
               // adapted from boost::hash_combine
               return acc ^ (MurmurHash64A(val) + 0x517cc1b727220a95ul + (acc << 6) + (acc >> 2));
               // bad if keys are the same!
               // return hashKey(val) ^ acc;
               });
      }

   inline Entry<K, V>* removeTag(uint64_t entryPtr) {
   }

   inline uint64_t addTag(uint64_t hash) {
   }
};
