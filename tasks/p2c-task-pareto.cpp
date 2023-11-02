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
      /*
      with limitedli as (
        select l_orderkey, l_quantity, l_discount
        from lineitem o
        where l_orderkey < 1000000
      )
      select l_orderkey, l_quantity, l_discount
      from limitedli o
      where not exists (select 1 from limitedli i where (i.l_quantity <= o.l_quantity and i.l_discount <= o.l_discount) and
                        (i.l_quantity < o.l_quantity or i.l_discount < o.l_discount))
      order by l_orderkey
      */
      auto l = make_unique<Scan>("lineitem");
      IU* lo = l->getIU("l_orderkey");
      IU* lq = l->getIU("l_quantity");
      IU* ld = l->getIU("l_discount");

      auto sel = make_unique<Selection>(
         std::move(l), makeCallExp("std::less()", lo, 1000000));

      auto p = make_unique<Pareto>(std::move(sel), std::vector<IU*>({lq, ld}));      

      auto sort = make_unique<Sort>(std::move(p), std::vector<IU*>({lo}));
      produceAndPrint(std::move(sort), {lo, lq, ld}, 2, 1781, 10); // limits the ouptut (don't use this for optimization)
   }
   return 0;
}
