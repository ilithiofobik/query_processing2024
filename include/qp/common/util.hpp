#pragma once
#include <sys/mman.h>
#include <cstdint>

namespace qp {


    inline uint64_t MurmurHash64A(uint64_t k) {
        // MurmurHash64A
        const uint64_t m = 0xc6a4a7935bd1e995;
        const int r = 47;
        uint64_t h = 0x8445d61a4e774912 ^ (8 * m);
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
   
    inline uint64_t MurmurHash64A(const void * key, int len)
    {
       const uint64_t m = 0xc6a4a7935bd1e995;
       const int r = 47;

       uint64_t h = 0x8445d61a4e774912 ^ (len * m);

       const uint64_t * data = (const uint64_t *)key;
       const uint64_t * end = data + (len/8);

       while(data != end)
       {
          uint64_t k = *(data++);

          k *= m;
          k ^= k >> r;
          k *= m;

          h ^= k;
          h *= m;
       }

       const unsigned char * data2 = (const unsigned char*)data;

       switch(len & 7)
       {
          case 7: h ^= uint64_t(data2[6]) << 48;
          case 6: h ^= uint64_t(data2[5]) << 40;
          case 5: h ^= uint64_t(data2[4]) << 32;
          case 4: h ^= uint64_t(data2[3]) << 24;
          case 3: h ^= uint64_t(data2[2]) << 16;
          case 2: h ^= uint64_t(data2[1]) << 8;
          case 1: h ^= uint64_t(data2[0]);
                  h *= m;
       };

       h ^= h >> r;
       h *= m;
       h ^= h >> r;

       return h;
    }



    // used for buggyhashtable only. for other code, use memory.hpp instead
    inline void* allocZeros(uint64_t size) {
        return mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    }

    template <std::size_t N, class F, std::size_t Cur = 0>
    inline void repeat(const F& f) {
        if constexpr (N == 0) {
            return;
        } else {
            f(Cur);
            repeat<N - 1, F, Cur + 1>(f);
        }
    }
}  // namespace qp
