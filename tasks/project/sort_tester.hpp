#include <iostream>
#include <vector>

#include "generate_runs.hpp"
#include "merge_pq_tester.hpp"

#pragma once

class SortTester {
   public:
    static void doExperiment(std::vector<int> runSizes, int numOfTests) {
        std::cout << "SortTester" << std::endl;

        for (int n : runSizes) {
            uint64_t comparisonCountQuickSort = 0;
            uint64_t comparisonCountLoserTree = 0;

            for (int i = 0; i < numOfTests; i++) {
                auto input = generateRandomTupleRun<int>(
                    1, n, 1000);  // n runs of size 1
                loserTreeSort(
                    input,
                    compare_count<std::tuple<int>>(comparisonCountLoserTree),
                    n);
            }

            for (int i = 0; i < numOfTests; i++) {
                auto input = generateRandomRun(n, 1000);
                quicksort(input, compare_count<int>(comparisonCountQuickSort));
            }

            float avgQS =
                static_cast<float>(comparisonCountQuickSort) / numOfTests;
            float avgLoserTree =
                static_cast<float>(comparisonCountLoserTree) / numOfTests;
            float ratio = avgLoserTree / avgQS;

            std::cout << "For run size " << n << ":\n";
            std::cout << "QS comparisons: " << avgQS << "\n";
            std::cout << "LoserTree Sort comparisons: " << avgLoserTree << "\n";
            std::cout << "Ratio: " << ratio << "\n\n";
        }
    }

   private:
    template <typename T>
    static void quicksortRec(std::vector<T>& vec, int low, int high,
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
    static void quicksort(std::vector<T>& vec, compare_count<T> comp) {
        if (!vec.empty()) {
            quicksortRec(vec, 0, vec.size() - 1, comp);
        }
    }

    template <typename... Args>
    static void loserTreeSort(
        std::vector<std::vector<std::tuple<Args...>>>& vec,
        compare_count<std::tuple<Args...>> comp, size_t n) {
        while (vec.size() > 1) {
            std::vector<std::vector<std::tuple<Args...>>> newVec = {};

            for (size_t chunk_start = 0; chunk_start < vec.size();
                 chunk_start += n) {
                std::vector<std::vector<std::tuple<Args...>>> chunk = {};
                for (size_t i = chunk_start;
                     i < std::min(chunk_start + n, vec.size()); i++) {
                    chunk.push_back(vec[i]);
                }
                std::vector<std::tuple<Args...>> sortedChunk =
                    MergePQTester::mergeLoserTreePQ(chunk, comp);
                newVec.push_back(sortedChunk);
            }

            vec = newVec;
        }
    }
};
