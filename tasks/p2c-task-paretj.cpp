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

using namespace std;
using namespace fmt;
using namespace p2c;

int main()
{
   {
      auto l = make_unique<Scan>("lineitem");
      IU* lo = l->getIU("l_orderkey");
      IU* lq = l->getIU("l_quantity");
      IU* ld = l->getIU("l_extendedprice");

      auto sel1 = make_unique<Selection>(
         std::move(l), makeCallExp("std::less()", lo, 10000000));

      auto sel2 = make_unique<Selection>(
         std::move(sel1), makeCallExp("([](auto& l, auto& r){ return l*r > 400000;})", make_unique<IUExp>(lq), make_unique<IUExp>(ld)));

      auto p = make_unique<Pareto>(std::move(sel2), std::vector<IU*>({lq, ld}));
      auto sort = make_unique<Sort>(std::move(p), std::vector<IU*>({lo}));
      produceAndPrint(std::move(sort), {lo, lq, ld}, 2, 36, 10); // limits the ouptut (don't use this for optimization)
   }
   return 0;
}
