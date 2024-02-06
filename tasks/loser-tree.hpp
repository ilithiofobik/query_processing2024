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
    size_t size;                         // Number of runs
    compare_count<int> comp;

   public:
    LoserTree(const std::vector<std::vector<int>>& runs,
              compare_count<int> comp)
        : size(runs.size()), comp(comp) {
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

    // Extract the next element from the tree
    int next(const std::vector<std::vector<int>>& runs) {
        // Implementation to extract the next element and adjust the tree
        int result = internal[0].value;
        size_t runNumber = internal[0].runNumber;
        external[runNumber].vectorIdx += 1;

        if (external[runNumber].vectorIdx < runs[runNumber].size()) {
            external[runNumber].value =
                runs[runNumber][external[runNumber].vectorIdx];

        } else {
            external[runNumber].value = std::numeric_limits<int>::max();
        }

        // rebalance tree
        int currentWinner = external[runNumber].value;
        size_t idx = (runNumber + size) / 2;

        update(idx, currentWinner, runNumber);

        return result;
    }

    void update(size_t idx, int currentWinner, size_t runNumber) {
        if (idx == 0) {
            internal[idx].value = currentWinner;
            internal[idx].runNumber = runNumber;
            return;
        }

        if (comp(currentWinner, internal[idx].value)) {
            std::swap(currentWinner, internal[idx].value);
            std::swap(runNumber, internal[idx].runNumber);
        }
        update(idx / 2, currentWinner, runNumber);
    }

    std::tuple<int, size_t> getSubtreeWinner(size_t idx) {
        // last layer, connected to external
        if (idx >= size / 2) {
            int leftIdx = idx * 2 - size;
            int rightIdx = idx * 2 + 1 - size;

            if (comp(external[leftIdx].value, external[rightIdx].value)) {
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
        return internal[0].value == std::numeric_limits<int>::max();
    }
};
