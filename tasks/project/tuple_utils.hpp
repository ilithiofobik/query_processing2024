#include <tuple>

#pragma once

template <unsigned I = 0, typename... Args>
constexpr inline static uint64_t calc_ovc(const std::tuple<Args...>& tuple1,
                                          const std::tuple<Args...>& tuple2) {
    if constexpr (I == sizeof...(Args)) {
        return 0;
    } else {
        auto val1 = get<I>(tuple1);
        auto val2 = get<I>(tuple2);
        if (val1 != val2 || I == sizeof...(Args) - 1) {
            uint64_t v = (uint64_t)get<I>(tuple1);
            return 1000 * (I + 1) - v;
        } else {
            return calc_ovc<I + 1>(tuple1, tuple2);
        }
    }
}

template <unsigned I = 0, typename... Args>
constexpr inline static void setToValue(std::tuple<Args...>& tuple,
                                        uint64_t value) {
    if constexpr (I < sizeof...(Args)) {
        get<I>(tuple) = value;
        setToValue<I + 1>(tuple, value);
    }
}
