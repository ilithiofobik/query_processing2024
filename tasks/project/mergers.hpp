#include <iostream>
#include <queue>
#include <vector>

#include "compare_count.hpp"
#include "loser-tree.hpp"

#pragma once

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
std::vector<int> mergeLoserTreePQ(const std::vector<std::vector<int>>& runs,
                                  compare_count<std::tuple<int>> comp) {
    LoserTree lt(runs, comp);
    std::vector<int> result;
    int previous = -1;
    while (!lt.empty()) {
        int next = lt.next(runs);
        result.push_back(next);
        if (next < previous) {
            std::cerr << "Error: " << next << " < " << previous << std::endl;
            return {};
        }
        previous = next;
    }
    return result;
}