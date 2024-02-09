#include <functional>
#include <iostream>
#include <queue>
#include <vector>

#include "tuple_utils.hpp"

#pragma once

template <typename... Args>
struct ExternalNodeOvc {
    std::tuple<Args...> value;
    size_t vectorIdx;
};

template <typename... Args>
struct InternalNodeOvc {
    std::tuple<Args...> value;
    size_t runNumber;
    uint64_t nodeOvc;
};

// Tree of Losers class
template <typename... Args>
class LoserTreeOvc {
    std::vector<ExternalNodeOvc<Args...>> external;  // Vector of external nodes
    std::vector<InternalNodeOvc<Args...>> internal;  // Vector of internal nodes
    size_t size;                                     // Number of runs
    compare_count_tuple<Args...> comp;               // Tuple comparison
    compare_count<uint64_t> compOvc;                 // OVC comparison
    std::tuple<Args...> maxTuple;                    // Maximum tuple

   public:
    LoserTreeOvc(const std::vector<std::vector<std::tuple<Args...>>>& runs,
                 compare_count_tuple<Args...> comp,
                 compare_count<uint64_t> compOvc)
        : size(runs.size()), comp(comp), compOvc(compOvc) {
        // Allocate memory for internal and external nodes
        // Initialize the tree with the first elements of each run

        internal.reserve(size);
        external.reserve(size);

        maxTuple = {};
        setToValue(maxTuple, 1000);

        for (size_t i = 0; i < size; i++) {
            ExternalNodeOvc en = {runs[i][0], 0};
            InternalNodeOvc in = {maxTuple, 0, 0};
            external.push_back(en);
            internal.push_back(in);
        }

        std::tuple<std::tuple<Args...>, size_t, uint64_t> winner =
            getSubtreeWinner(1);
        internal[0].value = std::get<0>(winner);
        internal[0].runNumber = std::get<1>(winner);
        internal[0].nodeOvc = std::get<2>(winner);
    }

    // Extract the next element from the tree
    std::tuple<Args...> next(
        const std::vector<std::vector<std::tuple<Args...>>>& runs) {
        // Implementation to extract the next element and adjust the tree
        std::tuple<Args...> result = internal[0].value;
        size_t runNumber = internal[0].runNumber;
        external[runNumber].vectorIdx += 1;

        if (external[runNumber].vectorIdx < runs[runNumber].size()) {
            external[runNumber].value =
                runs[runNumber][external[runNumber].vectorIdx];

        } else {
            external[runNumber].value = maxTuple;
        }

        // rebalance tree
        std::tuple<Args...> currentWinner = external[runNumber].value;
        size_t idx = (runNumber + size) / 2;

        auto currentWinnerOvc = calc_ovc(internal[idx].value, currentWinner);
        update(idx, currentWinner, runNumber, currentWinnerOvc);

        return result;
    }

    // Update after new leaf is added
    void update(size_t idx, std::tuple<Args...> currentWinner, size_t runNumber,
                uint64_t ovc) {
        if (idx == 0) {
            internal[idx].value = currentWinner;
            internal[idx].runNumber = runNumber;
            internal[idx].nodeOvc = ovc;
            return;
        }

        if (compOvc(internal[idx].nodeOvc, ovc)) {
            std::swap(currentWinner, internal[idx].value);
            std::swap(runNumber, internal[idx].runNumber);
            std::swap(ovc, internal[idx].nodeOvc);
        } else if (!compOvc(ovc, internal[idx].nodeOvc)) {
            if (comp(internal[idx].value, currentWinner)) {
                std::swap(currentWinner, internal[idx].value);
                std::swap(runNumber, internal[idx].runNumber);
                std::swap(ovc, internal[idx].nodeOvc);
                internal[idx].nodeOvc =
                    calc_ovc(internal[idx].value, currentWinner);
            }
        }

        update(idx / 2, currentWinner, runNumber, ovc);
    }

    // Get winner of the subtree, update the subtree
    std::tuple<std::tuple<Args...>, size_t, uint64_t> getSubtreeWinner(
        size_t idx) {
        // last layer, connected to external
        if (idx >= size / 2) {
            int leftIdx = idx * 2 - size;
            int rightIdx = idx * 2 + 1 - size;

            if (comp(external[leftIdx].value, external[rightIdx].value)) {
                internal[idx].value = external[leftIdx].value;
                internal[idx].runNumber = leftIdx;
                internal[idx].nodeOvc =
                    calc_ovc(external[leftIdx].value, external[rightIdx].value);

                return {external[rightIdx].value, rightIdx,
                        internal[idx].nodeOvc};
            } else {
                internal[idx].value = external[rightIdx].value;
                internal[idx].runNumber = rightIdx;
                internal[idx].nodeOvc =
                    calc_ovc(external[leftIdx].value, external[rightIdx].value);

                return {external[leftIdx].value, leftIdx,
                        internal[idx].nodeOvc};
            }
        }

        // we are in upper layers
        auto left = getSubtreeWinner(idx * 2);
        auto right = getSubtreeWinner(idx * 2 + 1);

        internal[idx].nodeOvc = calc_ovc(std::get<0>(left), std::get<0>(right));

        if (comp(std::get<0>(right), std::get<0>(left))) {
            internal[idx].value = std::get<0>(right);
            internal[idx].runNumber = std::get<1>(right);

            return left;
        } else {
            internal[idx].value = std::get<0>(left);
            internal[idx].runNumber = std::get<1>(left);

            return right;
        }
    }

    // Check if the tree is empty (all runs are exhausted)
    bool empty() const {
        // Implementation to check if the tree is empty
        return internal[0].value == maxTuple;
    }
};
