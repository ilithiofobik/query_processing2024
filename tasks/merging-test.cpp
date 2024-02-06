#include <functional>
#include <iostream>
#include <queue>
#include <random>
#include <vector>

#define P 16  // number of sorted runs

struct KnuthNode {
    int key;
    int record;
    int loser;
    int rn;
    int pe;
    int pi;
};

// A node for the tree-of-losers priority queue
struct TreeNode {
    int value;
    int runIndex;
    bool operator>(const TreeNode& other) const { return value > other.value; }
};

// Function to count comparisons for std::priority_queue
template <typename T>
struct compare_count {
    int& count;
    compare_count(int& count) : count(count) {}
    bool operator()(const T& a, const T& b) {
        ++count;
        return a > b;  // sorts in ascending order
    }
};

// Generate sorted runs with random numbers
std::vector<std::vector<int>> generateSortedRuns(int runSize, int numRuns) {
    std::vector<std::vector<int>> runs(numRuns, std::vector<int>(runSize));
    std::random_device rd;
    std::mt19937 mt(rd());  // mersenne twister engine
    std::uniform_int_distribution<int> dist(0, 1000000);

    for (auto& run : runs) {
        for (auto& val : run) {
            val = dist(mt);
        }
        std::sort(run.begin(), run.end());
    }

    return runs;
}

// Merge runs using standard priority queue
// Merge runs using standard priority queue
int mergeStandardPQ(const std::vector<std::vector<int>>& runs,
                    int& comparisonCount) {
    std::priority_queue<std::tuple<int, size_t>,
                        std::vector<std::tuple<int, size_t>>,
                        compare_count<std::tuple<int, size_t>>>
        pq{compare_count<std::tuple<int, size_t>>(comparisonCount)};

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

    return comparisonCount;
}

// Merge runs using tree-of-losers priority queue
int mergeLoserTreePQ(const std::vector<std::vector<int>>& runs,
                     int& comparisonCount) {
    std::priority_queue<TreeNode, std::vector<TreeNode>, std::greater<TreeNode>>
        tree;
    std::vector<size_t> indices(runs.size(), 0);
    for (size_t i = 0; i < runs.size(); ++i) {
        tree.push({runs[i][0], static_cast<int>(i)});
    }

    while (!tree.empty()) {
        TreeNode minNode = tree.top();
        tree.pop();
        comparisonCount++;

        // Increment the index for the corresponding run
        indices[minNode.runIndex]++;

        if (indices[minNode.runIndex] < runs[minNode.runIndex].size()) {
            tree.push({runs[minNode.runIndex][indices[minNode.runIndex]],
                       minNode.runIndex});
        }
    }
    return comparisonCount;
}

int main() {
    std::vector<int> runSizes = {1000, 10000, 100000};
    int numRuns = 8;
    int numOfTests = 10;

    for (int n : runSizes) {
        int comparisonCountStandard = 0;
        int comparisonCountLoserTree = 0;

        for (int i = 0; i < numOfTests; i++) {
            auto runs = generateSortedRuns(n, numRuns);

            mergeStandardPQ(runs, comparisonCountStandard);
            //   mergeLoserTreePQ(runs, comparisonCountLoserTree);
        }

        float avgStandard =
            static_cast<float>(comparisonCountStandard) / numOfTests;
        float avgLoserTree =
            static_cast<float>(comparisonCountLoserTree) / numOfTests;

        std::cout << "For run size " << n << ":\n";
        std::cout << "Standard PQ comparisons: " << avgStandard << "\n";
        std::cout << "LoserTree PQ comparisons: " << avgLoserTree << "\n\n";
    }

    return 0;
}
