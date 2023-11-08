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
    * select o_orderkey, ps_partkey, ps_suppkey, o_totalprice,  l_extendedprice, ps_supplycost
    * from orders
    *         join lineitem on o_orderkey = l_orderkey
    *         join partsupp on ps_partkey = l_partkey and ps_suppkey = l_suppkey
    * where o_totalprice * 1.03 < l_extendedprice  and l_extendedprice * 1.03 < ps_supplycost
    */
   {
      auto o = make_unique<ParallelScan>("orders");
      IU* otp = o->getIU("o_totalprice");
      IU* ook = o->getIU("o_orderkey");
      auto l = make_unique<ParallelScan>("lineitem");
      IU* lok = l->getIU("l_orderkey");
      IU* lep = l->getIU("l_extendedprice");
      IU* lpk = l->getIU("l_partkey");
      IU* lsk = l->getIU("l_suppkey");
      auto p = make_unique<ParallelScan>("partsupp");
      IU* ppk = p->getIU("ps_partkey");
      IU* psk = p->getIU("ps_suppkey");
      IU* psc = p->getIU("ps_supplycost");

      auto hj1 = make_unique<ParallelHashJoin>(std::move(p), std::move(l), vector<IU*>{ppk, psk}, vector<IU*>{lpk, lsk});
   
      auto sel1 = make_unique<ParallelSelection>(
         std::move(hj1), makeCallExp("([](auto& lep, auto& psc){ return lep * 1.03 < psc;})", 
            make_unique<IUExp>(lep), make_unique<IUExp>(psc)));

      auto hj2 = make_unique<ParallelHashJoin>(
         std::move(o), std::move(sel1), vector<IU*>{ook}, vector<IU*>{lok});
   
      auto sel2 = make_unique<ParallelSelection>(
         std::move(hj2), makeCallExp("([](auto& otp, auto& lep){ return otp * 1.03 < lep;})", 
            make_unique<IUExp>(otp), make_unique<IUExp>(lep)));

      produceAndSynchronizedPrint(std::move(sel2), {ook, ppk, psk, otp, lep, psc});
   }
   return 0;
}
