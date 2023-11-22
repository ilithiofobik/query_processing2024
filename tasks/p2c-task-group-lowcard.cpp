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
   /*
   */
   {
      auto l = make_unique<ParallelScan>("lineitem");
      IU* lrf = l->getIU("l_returnflag");

      auto gb = make_unique<ParallelGroupBy>(std::move(l), IUSet({lrf}));
      gb->addCount("cnt");

      IU* cnt = gb->getIU("cnt");
      produceAndSynchronizedPrint(std::move(gb), {lrf, cnt});
   }
   
   return 0;
}
