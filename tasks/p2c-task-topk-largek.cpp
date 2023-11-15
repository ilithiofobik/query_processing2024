#include <iostream>
#include <string>
#include <map>
#include <unordered_map>
#include <vector>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <source_location>
#include <string>
#include <string_view>
#include <sstream>

#include "p2c/foundations.hpp"
#include "p2c/types.hpp"
#include "p2c/tpch.hpp"
#include "additionaloperators.hpp"
#include "paralleloperators.hpp"

using namespace std;
using namespace fmt;
using namespace p2c;

int main()
{
   {
      auto l = make_unique<ParallelScan>("orders");
      IU* ok = l->getIU("o_orderkey");
      IU* tp = l->getIU("o_totalprice");
      IU* ck = l->getIU("o_custkey");

      auto topK = make_unique<ParallelTopK>(std::move(l), std::vector<IU*>({ok, tp, ck}), 100000);
      produceAndSynchronizedPrint(std::move(topK), {ok, tp, ck}, 0, 10); // limits the ouptut (don't use this for optimization)
   }
   return 0;
}