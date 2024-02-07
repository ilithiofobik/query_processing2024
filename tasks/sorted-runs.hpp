#include <random>
#include <vector>

std::vector<int> generatreRandomRun(int runSize) {
    std::vector<int> run(runSize);
    std::random_device rd;
    std::mt19937 mt(rd());  // mersenne twister engine
    std::uniform_int_distribution<int> dist(0, 1000000);

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
    std::uniform_int_distribution<int> dist(0, 1000000);

    for (auto& run : runs) {
        for (auto& val : run) {
            val = dist(mt);
        }
        std::sort(run.begin(), run.end());
    }

    return runs;
}
