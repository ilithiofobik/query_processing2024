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
   /*
   select count(*)
   from orders
   where o_orderdate < '1995-03-15'::date
   */
   {
      std::cout << "//" << stringToType<date>("1995-03-15", 10) << std::endl;
      auto o = make_unique<Scan>("orders");
      IU* od = o->getIU("o_orderdate");
      IU* op = o->getIU("o_totalprice");

      auto sel = make_unique<Selection>(
         std::move(o), makeCallExp("std::less()", od, stringToType<date>("1995-03-15", 10).value));
      auto gb = make_unique<GroupBy>(std::move(sel), IUSet());
      gb->addCount("cnt");
      gb->addSum("sum1", op);

      IU* cnt = gb->getIU("cnt");
      IU* sum1 = gb->getIU("sum1");
      produceAndPrint(std::move(gb), {cnt, sum1});
   }
   
   return 0;
}
