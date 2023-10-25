#include <functional>
#include <tuple>
#include <algorithm>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <cassert>
#include <qp/common/perfevent.hpp>
#include "p2c/tpch.hpp"

using namespace std;
using namespace p2c;

#ifndef TASK_NAME
#define NAME "none"
#else
#define NAME TASK_NAME
#endif

int main(int argc, char** argv) {
   string tpch_folder = argc >= 2 ? argv[1] : "/opt/tpch-p2c/sf1";
   TPCH db(tpch_folder);
   unsigned run_count = argc >= 3 ? atoi(argv[2]) : 1;
   auto tpch_scale = tpch_folder.substr(tpch_folder.find_last_of('/') + 1);

   BenchmarkParameters params;
   params.setParam("task", NAME);
   params.setParam("workload", tpch_scale);
   params.setParam("variant", "");
   params.setParam("impl", "hyper");

   for (unsigned run = 0; run < run_count; ++run) {
    {
      PerfEventBlock perf(1, params, run == 0);
      #include "p2c-query.cpp"
    }
   }
   return 0;
}