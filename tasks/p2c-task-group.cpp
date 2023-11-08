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
   select o_orderdate, count(*), avg(o_totalprice), max(o_custkey)
   from orders
   where o_orderdate < '1995-03-15'::date
   group by o_orderdate
   order by o_orderdate
   */
   {
      std::cout << "//" << stringToType<date>("1995-03-15", 10) << std::endl;
      auto o = make_unique<Scan>("orders");
      IU* od = o->getIU("o_orderdate");
      IU* op = o->getIU("o_totalprice");
      IU* ok = o->getIU("o_custkey");

      auto sel = make_unique<Selection>(
         std::move(o), makeCallExp("std::less()", od, stringToType<date>("1995-03-15", 10).value));
      auto gb = make_unique<GroupBy>(std::move(sel), IUSet({od}));
      gb->addCount("cnt");
      gb->addAvg("avg", op);
      gb->addMax("max", ok);

      IU* cnt = gb->getIU("cnt");
      IU* max = gb->getIU("max"); // Known Issue: You can't reuse the same scan operator
      IU* avg = gb->getIU("avg");
      auto sort = make_unique<Sort>(std::move(gb), std::vector<IU*>({od}));
      produceAndPrint(std::move(sort), {od, cnt, avg, max});
   }
   
   return 0;
}
