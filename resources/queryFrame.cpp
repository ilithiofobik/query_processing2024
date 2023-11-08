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
#include "../tasks/p2c-queryframe-infra.hpp"

using namespace std;
using namespace p2c;

#define DO_EXPAND(VAL)  VAL ## 1
#define EXPAND(VAL)     DO_EXPAND(VAL)
#define DO_QUOTE(X)     #X
#define QUOTE(X)        DO_QUOTE(X)

#if !defined(TASK_NAME) || (EXPAND(TASK_NAME) == 1)
#define TASK_NAME_OD "none"
#else
#define TASK_NAME_OD QUOTE(TASK_NAME)
#endif

#if !defined(VARIANT_NAME) || (EXPAND(VARIANT_NAME) == 1)
#define VARIANT_NAME_OD "default"
#else
#define VARIANT_NAME_OD QUOTE(VARIANT_NAME)
#endif

int main(int argc, char** argv) {
   string tpch_folder = argc >= 2 ? argv[1] : "/opt/tpch-p2c/sf1";
   TPCH db(tpch_folder);
   unsigned run_count = argc >= 3 ? atoi(argv[2]) : 1;
   auto tpch_scale = tpch_folder.substr(tpch_folder.find_last_of('/') + 1);

   BenchmarkParameters params;
   params.setParam("task", TASK_NAME_OD);
   params.setParam("workload", tpch_scale);
   params.setParam("variant", VARIANT_NAME_OD);
   params.setParam("impl", "hyper");

   for (unsigned run = 0; run < run_count; ++run) {
    {
      PerfEventBlock perf(1, params, run == 0);
      #include "p2c-query.cpp"
    }
   }
   return 0;
}
