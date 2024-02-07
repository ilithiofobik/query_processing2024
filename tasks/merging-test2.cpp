#include <iostream>
#include <queue>
#include <vector>

#include "mergers2.hpp"
#include "sorted-runs.hpp"

int main() {
    std::vector<int> runSizes = {1000, 10000, 100000, 1000000};
    int numRuns = 16;
    int numOfTests = 100;

    for (int n : runSizes) {
        uint64_t comparisonCountStandard = 0;
        uint64_t comparisonCountLoserTree = 0;

        for (int i = 0; i < numOfTests; i++) {
            auto runs = generateSortedTupleRuns(n, numRuns);
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
