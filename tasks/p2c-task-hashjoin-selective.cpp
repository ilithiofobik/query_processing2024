#include <fmt/core.h>
#include <fmt/ranges.h>

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
    * select o_totalprice, l_extendedprice
    * from orders
    *         join lineitem on o_totalprice = l_extendedprice and o_totalprice = l_suppkey
    */
   {
      auto o = make_unique<ParallelScan>("orders");
      IU* otp = o->getIU("o_totalprice");
      auto l = make_unique<ParallelScan>("lineitem");
      IU* lep = l->getIU("l_extendedprice");
      IU* lsk = l->getIU("l_suppkey");

      auto sel1 = make_unique<ParallelSelection>(
         std::move(o), makeCallExp("([](auto& otp){ return otp > 0;})", 
            make_unique<IUExp>(otp)));

      auto hj1 = make_unique<ParallelHashJoin>(std::move(sel1), std::move(l), vector<IU*>{otp, otp}, vector<IU*>{lep, lsk});

      produceAndSynchronizedPrint(std::move(hj1), {otp, lep}); 
 
   }
   return 0;
}
