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
      IU* lp = l->getIU("l_extendedprice");
      IU* ld = l->getIU("l_discount");

      auto sel = make_unique<Selection>(
         std::move(l), makeCallExp("([](auto& a1, auto& a2, auto& a3){ return a1*a2*a3 > 300000;})", 
            make_unique<IUExp>(lq), make_unique<IUExp>(lp), make_unique<IUExp>(ld)));

      auto p = make_unique<Pareto>(std::move(sel), std::vector<IU*>({lq, lp, ld}));
      auto sort = make_unique<Sort>(std::move(p), std::vector<IU*>({lo}));
      produceAndPrint(std::move(sort), {lo, lq, lp, ld}, 2, 95, 10); // limits the ouptut (don't use this for optimization)
   }
   return 0;
}
