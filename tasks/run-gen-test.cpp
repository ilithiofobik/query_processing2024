template <typename T>
std::vector<std::vector<T>> algorithmR(const std::vector<T>& input) {
    TreeOfLosers<T> tree;
    std::vector<std::vector<T>> runs;
    std::vector<T> currentRun;
    auto it = input.begin();

    // Initialize the tree with the first element if the input is not empty
    if (it != input.end()) {
        tree.insert(*it);
        ++it;
    }

    while (!tree.empty()) {
        T minElement = tree.extract_min();

        // If currentRun is empty or the minElement is not less than the last
        // element in currentRun
        if (currentRun.empty() || !(minElement < currentRun.back())) {
            currentRun.push_back(minElement);
        } else {
            // Finalize the current run and start a new one
            runs.push_back(currentRun);
            currentRun.clear();
            currentRun.push_back(minElement);
        }

        // Insert the next input element into the tree, if available
        if (it != input.end()) {
            tree.insert(*it);
            ++it;
        }
    }

    // Don't forget to add the last run
    if (!currentRun.empty()) {
        runs.push_back(currentRun);
    }

    return runs;
}

int main() {
    // Example usage
    std::vector<int> input = {5, 3, 6, 7, 2, 4, 9, 1, 8};
    auto runs = algorithmR(input);

    // Print the runs
    for (const auto& run : runs) {
        for (int val : run) {
            std::cout << val << " ";
        }
        std::cout << std::endl;
    }

    return 0;
}