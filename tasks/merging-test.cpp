#include <iostream>
#include <queue>
#include <vector>

#include "count-comparer.hpp"
#include "loser-tree.hpp"
#include "sorted-runs.hpp"

// Merge runs using standard priority queue
void mergeStandardPQ(const std::vector<std::vector<int>>& runs,
                     uint64_t& comparisonCount) {
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
                      uint64_t& comparisonCount) {
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
    std::vector<int> runSizes = {1000, 10000, 100000, 1000000};
    int numRuns = 16;
    int numOfTests = 100;

    for (int n : runSizes) {
        uint64_t comparisonCountStandard = 0;
        uint64_t comparisonCountLoserTree = 0;

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
