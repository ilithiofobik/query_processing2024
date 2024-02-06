#include <functional>
#include <iostream>
#include <queue>
#include <random>
#include <vector>

#include "count-comparer.hpp"
#include "loser-tree.hpp"

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

// Merge runs using standard priority queue
void mergeStandardPQ(const std::vector<std::vector<int>>& runs,
                     int& comparisonCount) {
    std::priority_queue<std::tuple<int, size_t>,
                        std::vector<std::tuple<int, size_t>>,
                        compare_count<std::tuple<int, size_t>>>
        pq{compare_count<std::tuple<int, size_t>>(comparisonCount)};

    size_t n = runs.size();
    std::vector<size_t> indices(n, 0);
    for (size_t i = 0; i < n; ++i) {
        pq.push({runs[i][0], i});
    }

    int previous = -1;

    while (!pq.empty()) {
        auto [minVal, minIndex] = pq.top();

        if (minVal < previous) {
            std::cerr << "Error: " << minVal << " < " << previous << std::endl;
            return;
        }
        previous = minVal;

        pq.pop();

        // Increment the index for the corresponding run
        indices[minIndex]++;

        if (indices[minIndex] < runs[minIndex].size()) {
            pq.push({runs[minIndex][indices[minIndex]], minIndex});
        }
    }
}

// Merge runs using tree-of-losers priority queue
void mergeLoserTreePQ(const std::vector<std::vector<int>>& runs,
                      int& comparisonCount) {
    LoserTree lt(runs, compare_count<int>(comparisonCount));

    int previous = -1;
    while (!lt.empty()) {
        int next = lt.next(runs);
        if (next < previous) {
            std::cerr << "Error: " << next << " < " << previous << std::endl;
            return;
        }
        previous = next;
    }
}

int main() {
    std::vector<int> runSizes = {100, 1000, 10000, 100000};
    int numRuns = 16;
    int numOfTests = 10;

    for (int n : runSizes) {
        int comparisonCountStandard = 0;
        int comparisonCountLoserTree = 0;

        for (int i = 0; i < numOfTests; i++) {
            auto runs = generateSortedRuns(n, numRuns);

            mergeStandardPQ(runs, comparisonCountStandard);
            mergeLoserTreePQ(runs, comparisonCountLoserTree);
        }

        float avgStandard =
            static_cast<float>(comparisonCountStandard) / numOfTests;
        float avgLoserTree =
            static_cast<float>(comparisonCountLoserTree) / numOfTests;
        float ratio = avgLoserTree / avgStandard;

        std::cout << "For run size " << n << ":\n";
        std::cout << "Standard PQ comparisons: " << avgStandard << "\n";
        std::cout << "LoserTree PQ comparisons: " << avgLoserTree << "\n";
        std::cout << "Ratio: " << ratio << "\n\n";
    }

    return 0;
}
