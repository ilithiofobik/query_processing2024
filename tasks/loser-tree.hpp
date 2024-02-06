#include <functional>
#include <iostream>
#include <queue>
#include <random>
#include <vector>

struct ExternalNode {
    int value;
    size_t vectorIdx;
};

struct InternalNode {
    int value;  // loser for all nodes but the root, root has global winner
    size_t runNumber;
};

// Tree of Losers class
class LoserTree {
    std::vector<ExternalNode> external;  // Vector of internal nodes
    std::vector<InternalNode> internal;  // Vector of internal nodes

    size_t size;  // Number of runs
    int comparisonCount;

   public:
    LoserTree(const std::vector<std::vector<int>>& runs)
        : size(runs.size()), comparisonCount(0) {
        // Allocate memory for internal and external nodes
        // Initialize the tree with the first elements of each run

        internal.reserve(size);  // first half internal, second half external
        external.reserve(size);

        for (size_t i = 0; i < size; i++) {
            ExternalNode en = {runs[i][0], 0};
            InternalNode in = {0, 0};  // Remove extra initializer values
            external.push_back(en);
            internal.push_back(in);
        }

        std::tuple<int, size_t> winner = getSubtreeWinner(1);
        internal[0].value = std::get<0>(winner);
        internal[0].runNumber = std::get<1>(winner);
    }

    int getComparisonCount() const { return comparisonCount; }

    // Extract the next element from the tree
    int next(const std::vector<std::vector<int>>& runs) {
        // Implementation to extract the next element and adjust the tree
        int result = internal[0].value;

        // replace value in external node
        size_t runNumber = internal[0].runNumber;
        size_t vectorIdx = external[runNumber].vectorIdx;
        size_t nextIdx = vectorIdx + 1;
        external[runNumber].vectorIdx = nextIdx;

        if (nextIdx < runs[runNumber].size()) {
            external[runNumber].value = runs[runNumber][nextIdx];

        } else {
            external[runNumber].value = std::numeric_limits<int>::max();
        }

        // rebalance tree
        int currentWinner = external[runNumber].value;
        size_t currentWinnerIdx = runNumber;
        size_t idx = runNumber / 2;

        while (idx > 0) {
            comparisonCount++;
            if (internal[idx].value < currentWinner) {
                std::swap(currentWinner, internal[idx].value);
                std::swap(runNumber, internal[idx].runNumber);
            }
            idx /= 2;
        }

        return result;
    }

    std::tuple<int, size_t> getSubtreeWinner(size_t idx) {
        // last layer, connected to external
        if (idx >= size / 2) {
            int leftIdx = idx * 2 - size;
            int rightIdx = idx * 2 + 1 - size;

            comparisonCount++;
            if (external[leftIdx].value > external[rightIdx].value) {
                internal[idx].value = external[leftIdx].value;
                internal[idx].runNumber = leftIdx;

                return {external[rightIdx].value, rightIdx};
            } else {
                internal[idx].value = external[rightIdx].value;
                internal[idx].runNumber = rightIdx;

                return {external[leftIdx].value, leftIdx};
            }
        }

        // we are in upper layers
        auto left = getSubtreeWinner(idx * 2);
        auto right = getSubtreeWinner(idx * 2 + 1);

        comparisonCount++;
        if (std::get<0>(left) < std::get<0>(right)) {
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
        return internal[0].value == std::numeric_limits<int>::max();
    }
};
