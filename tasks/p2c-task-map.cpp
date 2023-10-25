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
      auto n = make_unique<Scan>("nation");
      IU* nn = n->getIU("n_name");
      IU* nr = n->getIU("n_regionkey");

      auto sel = make_unique<Selection>(std::move(n), makeCallExp("std::less()", nr, 2));

      auto m = make_unique<Map>(std::move(sel), makeCallExp("std::plus()", nr, 5), "nrNew", Type::Integer); 
      IU* nrNew = m->getIU("nrNew");
      produceAndPrint(std::move(m), {nn, nr, nrNew});
   }
   return 0;
}