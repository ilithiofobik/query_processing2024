#include <functional>
#include <iostream>
#include <vector>

template <typename T>
struct InternalNode {
    T value;  // loser for all nodes but the root, root has global winner
    size_t runNumber;
};

// Tree of Losers class
template <typename T>
class SingleLoserTree {
    std::vector<T> external;                // Vector of external nodes
    std::vector<InternalNode<T>> internal;  // Vector of internal nodes
    size_t size;                            // Number of runs
    compare_count<T> comp;
    size_t idxCounter;

   public:
    SingleLoserTree(const std::vector<T>& input, compare_count<int> comp,
                    size_t size)
        : size(size), comp(comp), idxCounter(0) {
        // Allocate memory for internal and external nodes
        // Initialize the tree with the first elements of each run
        internal.reserve(size);  // first half internal, second half external
        external.reserve(size);

        for (size_t i = 0; i < size; i++) {
            T en = input[idxCounter];
            idxCounter++;
            InternalNode in = {0, 0};  // Remove extra initializer values
            external.push_back(en);
            internal.push_back(in);
        }

        std::tuple<int, size_t> winner = getSubtreeWinner(1);
        internal[0].value = std::get<0>(winner);
        internal[0].runNumber = std::get<1>(winner);
    }

    // Extract the next element from the tree
    T next(const std::vector<T>& run) {
        // Implementation to extract the next element and adjust the tree
        int result = internal[0].value;
        size_t externalIdx = internal[0].runNumber;

        if (idxCounter < run.size()) {
            external[externalIdx] = run[idxCounter];
            idxCounter++;
        } else {
            external[externalIdx] = std::numeric_limits<T>::max();
        }

        // rebalance tree
        T currentWinner = external[externalIdx];
        size_t idx = (externalIdx + size) / 2;

        update(idx, currentWinner, externalIdx);

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

            if (comp(external[leftIdx], external[rightIdx])) {
                internal[idx].value = external[leftIdx];
                internal[idx].runNumber = leftIdx;

                return {external[rightIdx], rightIdx};
            } else {
                internal[idx].value = external[rightIdx];
                internal[idx].runNumber = rightIdx;

                return {external[leftIdx], leftIdx};
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
        return internal[0].value == std::numeric_limits<T>::max();
    }
};
