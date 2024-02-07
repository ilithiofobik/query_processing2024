#include <iostream>
#include <queue>
#include <vector>

#include "count-comparer.hpp"
#include "loser-tree-ovc.hpp"
#include "loser-tree.hpp"

// Merge runs using tree-of-losers priority queue
std::vector<std::tuple<int, int, int, int, int>> mergeLoserTreePQOvc(
    const std::vector<std::vector<std::tuple<int, int, int, int, int>>>& runs,
    uint64_t& comparisonCount, uint64_t& comparisonCountOvc) {
    compare_count<std::tuple<int, int, int, int, int>> comp(comparisonCount);
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
std::vector<std::tuple<int, int, int, int, int>> mergeLoserTreePQPlain(
    const std::vector<std::vector<std::tuple<int, int, int, int, int>>>& runs,
    uint64_t& comparisonCount) {
    compare_count<std::tuple<int, int, int, int, int>> comp(comparisonCount);
    compare_count<uint64_t> compOvc(comparisonCount);
    LoserTreeOvc<int, int, int, int, int> lt(runs, comp, compOvc);
    std::vector<std::tuple<int, int, int, int, int>> result;
    while (!lt.empty()) {
        std::tuple<int, int, int, int, int> next = lt.next(runs);
        result.push_back(next);
    }
    return result;
}