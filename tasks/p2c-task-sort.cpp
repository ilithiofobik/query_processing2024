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
      IU* lq = l->getIU("l_quantity");
      IU* lls = l->getIU("l_linestatus");
      IU* lo = l->getIU("l_orderkey");

      auto sort = make_unique<Sort>(std::move(l), std::vector<IU*>({lq, lls}));
      produceAndPrint(std::move(sort), {lo, lq, lls}, 2, 99999, 10);
   }
   return 0;
}
