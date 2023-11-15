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
      auto l = make_unique<ParallelScan>("lineitem");
      IU* lq = l->getIU("l_quantity");
      IU* lp = l->getIU("l_extendedprice");
      IU* lo = l->getIU("l_orderkey");

      auto topK = make_unique<ParallelTopK>(std::move(l), std::vector<IU*>({lq, lp, lo}), 10);
      produceAndSynchronizedPrint(std::move(topK), {lq, lp, lo}); // limits the ouptut (don't use this for optimization)
   }
   return 0;
}
