#pragma once

#include <cstdint>
#include <vector>

#include "qp/common/util.hpp"

using namespace qp;

const uint64_t m_i = 64;  // m=2^b
const double m_f = 64.0;
const uint64_t b = 8;
const double alpha = 0.709;
const double two_f = 2.0;
const double two_pow32 = 4294967296.0;

struct HyperLogLog {
    std::vector<int> m_arr;

    HyperLogLog() {
        m_arr.reserve(m_i);
        for (uint64_t i = 0; i < m_i; i++) {
            m_arr.push_back(0);
        }
    }

    ~HyperLogLog() {}

    // void init() {
    //     m_arr.reserve(m_i);
    //     for (uint64_t i = 0; i < m_i; i++) {
    //         m_arr.push_back(0);
    //     }
    // }

    void insert(uint64_t hash) {
        uint64_t j = hash >> (64 - b);
        uint64_t w = hash << b;
        m_arr[j] = std::max(m_arr[j], 1 + __builtin_clzl(w));
    }

    template <typename... Args>
    void insert_tuple(const std::tuple<Args...> &args) {
        uint64_t hash = hashKey(args);
        insert(hash);
    }

    double estimate() {
        double powers_sum = 0.0;

        for (const int &x : m_arr) {
            powers_sum += pow(two_f, -x);
        }

        double e = (alpha * m_f * m_f) / powers_sum;

        if (e <= 160.0) {
            int v = 0;
            for (const int &x : m_arr) {
                if (x == 0) {
                    v++;
                }
            }

            if (v != 0) {
                return m_f * log(m_f / ((double)v));
            } else {
                return e;
            }
        } else if (e > 143165576.533) {
            return -two_pow32 * log(1 - (e / two_pow32));
        } else {
            return e;
        }
    }

    void merge(HyperLogLog &other) {
        for (uint64_t i = 0; i < m_i; i++) {
            m_arr[i] = std::max(m_arr[i], other.m_arr[i]);
        }
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
                return acc ^ (MurmurHash64A(val) + 0x517cc1b727220a95ul +
                              (acc << 6) + (acc >> 2));
            });
    }
};
