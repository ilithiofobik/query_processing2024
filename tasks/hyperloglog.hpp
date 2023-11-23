#pragma once

#include <cstdint>
#include <vector>

const uint64_t m_i = 64;  // m=2^b
const double m_f = 64.0;
const uint64_t b = 8;
const double alpha = 0.709;
const double two_f = 2.0;
const double two_pow32 = 4294967296.0;

struct HyperLogLog {
    std::vector<int> m_arr;

    HyperLogLog() {}

    ~HyperLogLog() {}

    void init() {
        m_arr.reserve(m_i);
        for (uint64_t i = 0; i < m_i; i++) {
            m_arr.push_back(0);
        }
    }

    void insert(uint64_t hash) {
        uint64_t j = hash >> (64 - b);
        uint64_t w = hash << b;
        m_arr[j] = std::max(m_arr[j], 1 + __builtin_clzl(w));
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
};
