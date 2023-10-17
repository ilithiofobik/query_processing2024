#pragma once
#include <utility>

namespace qp {

    // inspired by https://go.dev/tour/flowcontrol/12
    template <typename F>
    struct [[nodiscard]] defer {
        F fn;

        explicit defer(F&& fn)
            : fn(std::move(fn)) {}

        ~defer() {
            try {
                fn();
            } catch (...) {}
        }
    };

}  // namespace qp
