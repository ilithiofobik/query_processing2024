#include <random>
#include <vector>

#define GLOAL_MAX 1000

std::vector<int> generatreRandomRun(int runSize) {
    std::vector<int> run(runSize);
    std::random_device rd;
    std::mt19937 mt(rd());  // mersenne twister engine
    std::uniform_int_distribution<int> dist(0, GLOAL_MAX);

    for (auto& val : run) {
        val = dist(mt);
    }

    return run;
}

// Generate sorted runs with random numbers
std::vector<std::vector<int>> generateSortedRuns(int runSize, int numRuns) {
    std::vector<std::vector<int>> runs(numRuns, std::vector<int>(runSize));
    std::random_device rd;
    std::mt19937 mt(rd());  // mersenne twister engine
    std::uniform_int_distribution<int> dist(0, GLOAL_MAX);

    for (auto& run : runs) {
        for (auto& val : run) {
            val = dist(mt);
        }
        std::sort(run.begin(), run.end());
    }

    return runs;
}

template <unsigned I = 0, typename... Args>
constexpr inline static void setToRand(std::tuple<Args...>& tuple, uint64_t min,
                                       uint64_t max) {
    if constexpr (I < sizeof...(Args)) {
        get<I>(tuple) = rand() % max + min;
        setToRand<I + 1>(tuple, min, max);
    }
}

std::vector<std::vector<std::tuple<int, int, int, int, int>>>
generateSortedTupleRuns(int runSize, int numRuns) {
    std::vector<std::vector<std::tuple<int, int, int, int, int>>> runs(
        numRuns, std::vector<std::tuple<int, int, int, int, int>>(runSize));
    std::random_device rd;
    std::mt19937 mt(rd());  // mersenne twister engine
    std::uniform_int_distribution<int> dist(0, GLOAL_MAX);

    for (auto& run : runs) {
        for (auto& val : run) {
            setToRand(val, 0, GLOAL_MAX);
        }
        std::sort(run.begin(), run.end());
    }

    return runs;
}