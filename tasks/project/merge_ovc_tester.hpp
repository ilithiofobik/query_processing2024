#include <iostream>
#include <queue>
#include <vector>

#include "compare_count.hpp"
#include "loser-tree-ovc.hpp"
#include "loser-tree.hpp"

#pragma once

class MergeOVCTester {
   public:
    static void doExperiment(std::vector<int> runSizes, int numRuns,
                             int numOfTests) {
        std::cout << "MergeOVCTester" << std::endl;

        for (int n : runSizes) {
            uint64_t comparisonCountLoserTree = 0;
            uint64_t comparisonCountLoserTreeOvc = 0;
            uint64_t comparisonOvc = 0;

            for (int i = 0; i < numOfTests; i++) {
                auto runs = generatedSortedTupleRun<int, int, int, int, int>(
                    n, numRuns);
                mergeLoserTreePQPlain(runs, comparisonCountLoserTree);
                mergeLoserTreePQOvc(runs, comparisonCountLoserTreeOvc,
                                    comparisonOvc);
            }

            float avgPlain =
                static_cast<float>(comparisonCountLoserTree) / numOfTests;
            float avgOvc = static_cast<float>(comparisonCountLoserTreeOvc +
                                              comparisonOvc) /
                           numOfTests;

            std::cout << "For run size " << n << ":\n";
            std::cout << "Plain LoserTree PQ comparisons: " << avgPlain << "\n";
            std::cout << "LoserTree PQ with OVC comparisons: " << avgOvc
                      << "\n\n";
        }
    }

   private:
    // Merge runs using tree-of-losers priority queue
    static std::vector<std::tuple<int, int, int, int, int>> mergeLoserTreePQOvc(
        const std::vector<std::vector<std::tuple<int, int, int, int, int>>>&
            runs,
        uint64_t& comparisonCount, uint64_t& comparisonCountOvc) {
        compare_count_tuple<int, int, int, int, int> comp(comparisonCount);
        compare_count<uint64_t> compOvc(comparisonCountOvc);
        LoserTreeOvc<int, int, int, int, int> lt(runs, comp, compOvc);
        std::vector<std::tuple<int, int, int, int, int>> result;
        while (!lt.empty()) {
            std::tuple<int, int, int, int, int> next = lt.next(runs);
            result.push_back(next);
        }
        return result;
    }

    // Merge runs using tree-of-losers priority queue
    static std::vector<std::tuple<int, int, int, int, int>>
    mergeLoserTreePQPlain(
        const std::vector<std::vector<std::tuple<int, int, int, int, int>>>&
            runs,
        uint64_t& comparisonCount) {
        compare_count_tuple<int, int, int, int, int> comp(comparisonCount);
        compare_count<uint64_t> compOvc(comparisonCount);
        LoserTreeOvc<int, int, int, int, int> lt(runs, comp, compOvc);
        std::vector<std::tuple<int, int, int, int, int>> result;
        while (!lt.empty()) {
            std::tuple<int, int, int, int, int> next = lt.next(runs);
            result.push_back(next);
        }
        return result;
    }
};
