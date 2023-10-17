#pragma once
#include <sys/mman.h>
#include <cstdint>

namespace qp {
    inline uint64_t hashKey(uint64_t k) {
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
