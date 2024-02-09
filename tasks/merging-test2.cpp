#include <iostream>
#include <queue>
#include <vector>

#include "project/mergers2.hpp"
#include "project/sorted_runs.hpp"

int main() {
    std::vector<int> runSizes = {1000, 10000, 100000, 1000000};
    int numRuns = 16;
    int numOfTests = 100;

    for (int n : runSizes) {
        uint64_t comparisonCountLoserTree = 0;
        uint64_t comparisonCountLoserTreeOvc = 0;
        uint64_t comparisonOvc = 0;

        for (int i = 0; i < numOfTests; i++) {
            auto runs = generateSortedTupleRuns(n, numRuns);
            mergeLoserTreePQPlain(runs, comparisonCountLoserTree);
            mergeLoserTreePQOvc(runs, comparisonCountLoserTreeOvc,
                                comparisonOvc);
        }

        float avgPlain =
            static_cast<float>(5 * comparisonCountLoserTree) / numOfTests;
        float avgOvc = static_cast<float>(5 * comparisonCountLoserTreeOvc +
                                          comparisonOvc) /
                       numOfTests;

        std::cout << "For run size " << n << ":\n";
        std::cout << "Plain LoserTree PQ comparisons: " << avgPlain << "\n";
        std::cout << "LoserTree PQ with OVC comparisons: " << avgOvc << "\n\n";

        return 0;
    }
}
