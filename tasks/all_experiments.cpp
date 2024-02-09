#include <iostream>
#include <queue>
#include <vector>

#include "project/generate_runs.hpp"
#include "project/merge_ovc_tester.hpp"
#include "project/merge_pq_tester.hpp"
#include "project/sort_tester.hpp"

int main() {
    MergePQTester::doExperiment({1000, 10000, 100000, 1000000}, 16, 100);
    MergeOVCTester::doExperiment({1000, 10000, 100000, 1000000}, 16, 100);
    SortTester::doExperiment({1000, 10000, 100000, 1000000}, 100);

    return 0;
}
