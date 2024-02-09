#include <iostream>
#include <vector>

#include "mergers.hpp"
#include "sorted-runs.hpp"

#pragma once

template <typename T>
void quicksortRec(std::vector<T>& vec, int low, int high,
                  compare_count<T> comp) {
    if (low < high) {
        // Partitioning index
        T pivot = vec[high];  // Choosing the last element as pivot
        int i = (low - 1);    // Index of smaller element

        for (int j = low; j <= high - 1; j++) {
            // If current element is smaller than or equal to pivot
            if (comp(vec[j], pivot)) {
                i++;  // Increment index of smaller element
                std::swap(vec[i], vec[j]);
            }
        }
        std::swap(vec[i + 1], vec[high]);

        int pi = i + 1;  // pi is partitioning index

        quicksortRec(vec, low, pi - 1,
                     comp);  // Recursively sort elements before partition
        quicksortRec(vec, pi + 1, high,
                     comp);  // Recursively sort elements after partition
    }
}

// Helper function to start the quicksort algorithm
template <typename T>
void quicksort(std::vector<T>& vec, compare_count<T> comp) {
    if (!vec.empty()) {
        quicksortRec(vec, 0, vec.size() - 1, comp);
    }
}

template <typename T>
void loserTreeSort(std::vector<std::vector<T>>& vec, compare_count<T> comp,
                   size_t n) {
    while (vec.size() > 1) {
        std::vector<std::vector<T>> newVec = {};

        for (size_t chunk_start = 0; chunk_start < vec.size();
             chunk_start += n) {
            std::vector<std::vector<T>> chunk = {};
            for (size_t i = chunk_start;
                 i < std::min(chunk_start + n, vec.size()); i++) {
                chunk.push_back(vec[i]);
            }
            std::vector<T> sortedChunk = mergeLoserTreePQ(chunk, comp);
            newVec.push_back(sortedChunk);
        }

        vec = newVec;
    }
}

// Function to calculate OVC as defined
int calculateOVC(const std::tuple<int, int, int, int, int>& a,
                 const std::tuple<int, int, int, int, int>& b) {
    int index = 1;
    for (int i = 0; i < 5; ++i) {
        if (std::get<i>(a) != std::get<i>(b)) {
            return 100 * (1 + index) -
                   std::abs(std::get<i>(a) - std::get<i>(b));
        }
        ++index;
    }
    return 0;  // Tuples are identical
}

int main() {
    std::vector<int> runSizes = {1000, 10000, 100000, 1000000};
    int numOfTests = 100;

    for (int n : runSizes) {
        uint64_t comparisonCountQuickSort = 0;
        uint64_t comparisonCountLoserTree = 0;

        for (int i = 0; i < numOfTests; i++) {
            auto input = generateSortedRuns(1, n);  // n runs of size 1
            loserTreeSort(input, compare_count<int>(comparisonCountLoserTree),
                          n);
        }

        for (int i = 0; i < numOfTests; i++) {
            auto input = generatreRandomRun(n);
            quicksort(input, compare_count<int>(comparisonCountQuickSort));
        }

        float avgQS = static_cast<float>(comparisonCountQuickSort) / numOfTests;
        float avgLoserTree =
            static_cast<float>(comparisonCountLoserTree) / numOfTests;
        float ratio = avgLoserTree / avgQS;

        std::cout << "For run size " << n << ":\n";
        std::cout << "QS comparisons: " << avgQS << "\n";
        std::cout << "LoserTree Sort comparisons: " << avgLoserTree << "\n";
        std::cout << "Ratio: " << ratio << "\n\n";
    }

    return 0;
}