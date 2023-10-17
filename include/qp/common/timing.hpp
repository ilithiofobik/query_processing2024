#pragma once
#include <chrono>

namespace qp {

    template <typename Unit = std::chrono::microseconds, typename Clock = std::chrono::steady_clock>
    struct [[nodiscard]] BlockTimer {
        using time_point_t = typename Clock::time_point;
        using output_t = typename Clock::rep;

        output_t& output;
        time_point_t start;

        explicit BlockTimer(output_t& ref)
            : output(ref)
            , start(Clock::now()) {}

        ~BlockTimer() {
            auto end = Clock::now();
            output = std::chrono::duration_cast<Unit>(end - start).count();
        }

        output_t until_now() {
            return std::chrono::duration_cast<Unit>(Clock::now() - start).count();
        }
    };
}  // namespace qp
