#include <random>
#include <tuple>
#include <vector>

#pragma once

std::vector<int> generateRandomRun(int runSize, int maxValue) {
    std::vector<int> run(runSize);
    std::random_device rd;
    std::mt19937 mt(rd());  // mersenne twister engine
    std::uniform_int_distribution<int> dist(0, maxValue);

    for (auto& val : run) {
        val = dist(mt);
    }

    return run;
}

// Generate sorted runs with random numbers
std::vector<std::vector<int>> generateSortedRuns(int runSize, int numRuns,
                                                 int maxValue) {
    std::vector<std::vector<int>> runs = {};

    for (int i = 0; i < numRuns; i++) {
        auto run = generateRandomRun(runSize, maxValue);
        std::sort(runs[i].begin(), runs[i].end());
        runs.push_back(run);
    }

    return runs;
}

// set tuple values to random numbers
template <unsigned I = 0, typename... Args>
constexpr inline static void setToRand(std::tuple<Args...>& tuple, uint64_t min,
                                       uint64_t max) {
    if constexpr (I < sizeof...(Args)) {
        std::get<I>(tuple) = rand() % max + min;
        setToRand<I + 1>(tuple, min, max);
    }
}

template <typename... Args>
std::vector<std::vector<std::tuple<Args...>>> generateRandomTupleRun(
    int runSize, int numRuns, int maxValue) {
    std::vector<std::vector<std::tuple<Args...>>> runs(
        numRuns, std::vector<std::tuple<Args...>>(runSize));

    for (auto& run : runs) {
        for (auto& val : run) {
            setToRand(val, 0, maxValue);
        }
    }

    return runs;
}

template <typename... Args>
std::vector<std::vector<std::tuple<Args...>>> generatedSortedTupleRun(
    int runSize, int numRuns) {
    std::vector<std::vector<std::tuple<Args...>>> runs =
        generateRandomTupleRun<Args...>(runSize, numRuns, 1000);

    for (auto& run : runs) {
        std::sort(run.begin(), run.end());
    }

    return runs;
}