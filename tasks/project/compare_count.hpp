#pragma once

// Function to count comparisons
template <typename T>
struct compare_count {
    uint64_t& count;
    compare_count(uint64_t& count) : count(count) {}
    bool operator()(const T& a, const T& b) {
        count++;
        return a > b;  // sorts in ascending order
    }
};

//  Function to count comparisons for tuples
template <typename... Args>
struct compare_count_tuple {
    uint64_t& count;
    compare_count_tuple(uint64_t& count) : count(count) {}

    template <unsigned I = 0>
    bool operator()(const std::tuple<Args...>& a,
                    const std::tuple<Args...>& b) {
        count++;

        if constexpr (I == sizeof...(Args)) {
            return false;
        } else {
            auto val1 = std::get<I>(a);
            auto val2 = std::get<I>(b);
            if (val1 != val2) {
                return val1 > val2;
            } else {
                return operator()<I + 1>(a, b);
            }
        }
    }
};
