#pragma once
#include <cstdint>
#include <vector>

#include "qp/common/util.hpp"

using namespace qp;

template <typename Data>
struct LocalBuffer {
    std::vector<Data> v;

    LocalBuffer() { v.reserve(10000); }

    ~LocalBuffer() {}

    void push_back(Data data) { v.push_back(data); }

    // struct iterator {};

    inline auto begin() const { return v.begin(); }
    inline const auto end() const { return v.end(); }

    uint64_t size() { return v.size(); }
};
