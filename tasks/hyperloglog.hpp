#pragma once

#include <cmath>
#include <cstdint>
#include <vector>

#include "qp/common/util.hpp"

using namespace qp;

const uint64_t b = 6;
const uint64_t m_i = 64;  // m=2^b
const double m_f = double(m_i);
const double alpha = 0.709;
const double two_pow32 = pow(2.0, 32.0);

struct HyperLogLog {
    std::vector<int> m_arr;

    HyperLogLog() {
        m_arr.reserve(m_i);
        for (uint64_t i = 0; i < m_i; i++) {
            m_arr.push_back(0);
        }
    }

    ~HyperLogLog() {}

    void insert(uint32_t hash) {
        uint32_t j = hash >> (32 - b);
        uint32_t w = hash << b;
        m_arr[j] = std::max(m_arr[j], 1 + __builtin_clzl(w));
    }

    double estimate() {
        double powers_sum = 0.0;

        for (const int &x : m_arr) {
            powers_sum += pow(2.0, -x);
        }

        double e = (alpha * m_f * m_f) / powers_sum;

        if (2.0 * e <= 5.0 * m_f) {
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
        } else if (e > two_pow32 / 30.0) {
            return -two_pow32 * log(1 - (e / two_pow32));
        } else {
            return e;
        }
    }

    uint64_t ht_size(uint64_t partCount) {
        return (uint64_t)std::ceil(estimate() / partCount);
    }

    void merge(HyperLogLog &other) {
        for (uint64_t i = 0; i < m_i; i++) {
            m_arr[i] = std::max(m_arr[i], other.m_arr[i]);
        }
    }
};
