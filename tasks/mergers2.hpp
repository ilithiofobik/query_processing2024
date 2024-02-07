#include <iostream>
#include <queue>
#include <vector>

#include "count-comparer.hpp"
#include "loser-tree-ovc.hpp"

// Merge runs using standard priority queue
void mergeStandardPQ(
    const std::vector<std::vector<std::tuple<int, int, int, int, int>>>& runs,
    uint64_t& comparisonCount) {
    std::priority_queue<
        std::tuple<std::tuple<int, int, int, int, int>, size_t>,
        std::vector<std::tuple<std::tuple<int, int, int, int, int>, size_t>>,
        compare_count<std::tuple<std::tuple<int, int, int, int, int>, size_t>>>
        pq{compare_count<
            std::tuple<std::tuple<int, int, int, int, int>, size_t>>(
            comparisonCount)};

    size_t n = runs.size();
    std::vector<size_t> indices(n, 0);
    for (size_t i = 0; i < n; ++i) {
        pq.push({runs[i][0], i});
    }

    while (!pq.empty()) {
        auto [minVal, minIndex] = pq.top();
        pq.pop();

        // Increment the index for the corresponding run
        indices[minIndex]++;

        if (indices[minIndex] < runs[minIndex].size()) {
            pq.push({runs[minIndex][indices[minIndex]], minIndex});
        }
    }
}

// Merge runs using tree-of-losers priority queue
std::vector<std::tuple<int, int, int, int, int>> mergeLoserTreePQ(
    const std::vector<std::vector<std::tuple<int, int, int, int, int>>>& runs,
    compare_count<std::tuple<int, int, int, int, int>> comp) {
    LoserTreeOvc<int, int, int, int, int> lt(runs, comp);
    std::vector<std::tuple<int, int, int, int, int>> result;
    while (!lt.empty()) {
        std::tuple<int, int, int, int, int> next = lt.next(runs);
        result.push_back(next);
    }
    return result;
}