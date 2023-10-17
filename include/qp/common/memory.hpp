#pragma once
#include <errno.h>
#include <sys/mman.h>
#include <cstdint>
#include <cstring>
#include <stdexcept>

#include <tbb/parallel_for.h>
#include <tbb/blocked_range.h>

namespace qp {

    struct mmap_allocator {
        inline static void* alloc(size_t size) {
            void* p =
                mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (p == MAP_FAILED) {
                auto err = errno;
                throw std::logic_error("Error during mmap: " + std::string(strerror(err)) +
                                       " while allocating " + std::to_string(1.0 * size / 1e6) +
                                       " GB");
            }
#if defined(__linux__)
            madvise(p, size, MADV_HUGEPAGE);
#endif
            return p;
        }

        inline static void dealloc(void* start, size_t size) {
            if (munmap(start, size)) {
                auto err = errno;
                throw std::logic_error("Error during munmap: " + std::string(strerror(err)) +
                                       " while deallocating " + std::to_string(1.0 * size / 1e6) +
                                       " GB");
            }
        }

        inline static void clear(void* start, size_t size) {
            static constexpr size_t grainsize = 4096;
            if (size <= grainsize) {
                memset(start, 0, size);
                return;
            }
            auto range = tbb::blocked_range<size_t>(0, size / grainsize);
            tbb::parallel_for(range, [&](const decltype(range)& r) {
                memset(reinterpret_cast<char*>(start) + (grainsize * r.begin()), 0, (grainsize * (r.end() - r.begin())));
            });
            if (auto rest = size % grainsize) {
                memset(reinterpret_cast<char*>(start) + size - rest, 0, rest);
            }
        }
    };

    using mem = mmap_allocator;
}  // namespace qp
