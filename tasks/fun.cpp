#include <algorithm>
#include <iostream>
#include <limits>
#include <random>
#include <vector>

// Node structure for external nodes
struct ExternalNode {
    int value;
    bool active;
};

// Tree of Losers implementation
class LoserTree {
   public:
    std::vector<int> internalNodes;

   private:
    // Indices of internal nodes (losers)
    std::vector<ExternalNode> leaves;  // External leaves (runs' heads)
    int size;

    // Rebalance the tree starting from the external node
    void rebalance(int index) {
        int parent = (index + size) / 2;
        while (parent > 0) {
            if (!leaves[index].active ||
                (leaves[internalNodes[parent]].active &&
                 leaves[internalNodes[parent]].value < leaves[index].value)) {
                std::swap(index, internalNodes[parent]);
            }
            parent /= 2;
        }
        internalNodes[0] = index;  // The winner (smallest element's index)
    }

   public:
    LoserTree(const std::vector<std::vector<int>>& runs) {
        size = runs.size();
        internalNodes.resize(size, 0);
        leaves.resize(size);

        // Initialize the leaves with the first element of each run
        for (int i = 0; i < size; ++i) {
            if (!runs[i].empty()) {
                leaves[i] = {runs[i][0], true};
            } else {
                leaves[i] = {std::numeric_limits<int>::max(), false};
            }
        }

        // Initialize internal nodes with a dummy value
        std::fill(internalNodes.begin(), internalNodes.end(), size);

        // Build the initial tree
        for (int i = size - 1; i >= 0; --i) {
            rebalance(i);
        }
    }

    // Get the next minimum element and update the tree
    int next(std::vector<std::vector<int>>& runs, std::vector<int>& indices,
             int& comparisons) {
        int winnerIndex = internalNodes[0];
        int winnerValue = leaves[winnerIndex].value;

        // Increment the index for the winning run
        indices[winnerIndex]++;
        if (indices[winnerIndex] < runs[winnerIndex].size()) {
            leaves[winnerIndex].value = runs[winnerIndex][indices[winnerIndex]];
        } else {
            leaves[winnerIndex].active = false;
            leaves[winnerIndex].value = std::numeric_limits<int>::max();
        }

        // Rebalance the tree after updating the winner run
        rebalance(winnerIndex);

        comparisons++;  // Increment the comparison count
        return winnerValue;
    }
};

int main() {
    // Sizes to test
    std::vector<int> sizes = {1000, 10000, 1000000};

    // For each size, generate runs, create a LoserTree, and count comparisons
    for (int size : sizes) {
        int comparisons = 0;
        std::vector<std::vector<int>> runs(8, std::vector<int>(size));

        // Random number generator to fill the runs with sorted data
        std::mt19937 gen(std::random_device{}());
        std::uniform_int_distribution<int> dist(0, 1000000);

        for (auto& run : runs) {
            std::generate(run.begin(), run.end(), [&] { return dist(gen); });
            std::sort(run.begin(), run.end());
        }

        // Create a LoserTree and merge the runs, counting comparisons
        std::vector<int> indices(8, 0);
        LoserTree tree(runs);
        while (indices[tree.internalNodes[0]] < size) {
            tree.next(runs, indices, comparisons);
        }

        // Output the results
        std::cout << "Size: " << size << ", Comparisons: " << comparisons
                  << std::endl;
    }

    return 0;
}
