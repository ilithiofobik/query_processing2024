#include <cassert>
#include <algorithm>
#include <functional>
#include <iostream>
#include <string>
#include <vector>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <string>
#include <string_view>
#include <sstream>
#include <p2c/io.hpp>
#include <p2c/tpch.hpp>
#include <p2c/types.hpp>
#include <p2c/foundations.hpp>

using namespace std;
using namespace fmt;
using namespace p2c;

// sort operator
struct Sort : public Operator {
   unique_ptr<Operator> input;
   vector<IU*> keyIUs;
   IU v{"vector", Type::Undefined};
   bool desc;

   char cmpSign = desc ? '>' : '<';

   // constructor
   Sort(unique_ptr<Operator> input, const vector<IU*>& keyIUs, bool desc = false) : input(std::move(input)), keyIUs(keyIUs), desc(desc)  {}

   // destructor
   ~Sort() {}

   IUSet availableIUs() override {
      return input->availableIUs();
   }

   void produce(const IUSet& required, ConsumerFn consume) override {
      IUSet requiredIUs = (required & input->availableIUs()) | IUSet(keyIUs);
      vector<IU*> requiredVec = requiredIUs.v;
      string requiredType = format("tuple<{}>", formatTypes(requiredVec));

      // create a vector containing tuples with all required values
      print("\nvector<{}> {};\n", requiredType, v.varname);

      input->produce(requiredIUs, [&]() {
         // push all required values to the vector
         print("{}.push_back({{{}}});\n", v.varname, formatVarnames(requiredVec));
      });

      // for every keyIU add it to list of required conditions indices
      vector<int> condIndices;
      for (auto it =  keyIUs.begin(); it != keyIUs.end(); it++) {
         condIndices.push_back(getReqIndex(requiredVec, *it));
      }

      // sort based on a comparer built with list of keyIU to compare
      print("\nsort({0}.begin(), {0}.end(), [](const {1}& a, const {1}& b) {{ return {2}; }});\n", 
         v.varname, 
         requiredType, 
         generateConditions(0, condIndices)
      );

      // provide the tuples as requiredIU to the consumer
      genBlock(format("for (auto it: {})", v.varname), [&]{
         for (long unsigned int i = 0; i < requiredVec.size(); i++) {
            provideIU(requiredVec[i], format("get<{}>(it)", i));
         }
         consume();
      });
   }

   private:
      string generateConditions(long unsigned int i, const vector<int>& v) {
         string ltCond = format("get<{0}>(a) {1} get<{0}>(b)", v[i], cmpSign);
         if (i == v.size() - 1) {
            // if it is the last condition just check if a.x < b.x
            return ltCond;
         } else {
            // if there are more conditions there are to cases
            // 1)  a.x < b.x
            // 2)  a.x == b.x and the rest of conditions are evaluated to true
            string nextConds = generateConditions(i + 1, v);
            return format("{0} || (get<{1}>(a) == get<{1}>(b) && ({2}))", ltCond, v[i], nextConds);
         }
      }

      // translate a keyIU to its index in requiredVec 
      int getReqIndex(const vector<IU*>& requiredVec, IU* iu) {
         auto itReq = find(requiredVec.begin(), requiredVec.end(), iu); 
         assert(itReq != requiredVec.end());
         return itReq - requiredVec.begin();
      }
};

// map operator (compute new value)
struct Map : public Operator {
   unique_ptr<Operator> input;
   unique_ptr<Exp> exp;
   IU iu;

   // constructor
   Map(unique_ptr<Operator> input, unique_ptr<Exp> exp, const string& name, Type type) : input(std::move(input)), exp(std::move(exp)), iu{name, type} {}

   // destructor
   ~Map() {}

   IUSet availableIUs() override {
      return input->availableIUs() | IUSet({&iu});
   }

   void produce(const IUSet& required, ConsumerFn consume) override {
      input->produce((required | exp->iusUsed()) - IUSet({&iu}), [&]() {
         // set the iu to exp value
         provideIU(&iu, exp->compile());
         // let the consumer take care of required + expIus without iu 
         // as the iu is not a part of the database
         consume();
      });
   }

   IU* getIU(const string& attName) {
      if (iu.name == attName)
         return &iu;
      throw;
   }
};

// group by operator
struct GroupBy : public Operator {
   enum AggFunction { Sum, Count, Max, AvgSum, AvgCnt };

   struct Aggregate {
      AggFunction aggFn; // aggregate function
      IU* inputIU; // IU to aggregate (is nullptr when aggFn==Count)
      IU resultIU;
   };

   unique_ptr<Operator> input;
   IUSet groupKeyIUs;
   vector<Aggregate> aggs;
   IU ht{"aggHT", Type::Undefined};

   // constructor
   GroupBy(unique_ptr<Operator> input, const IUSet& groupKeyIUs) : input(std::move(input)), groupKeyIUs(groupKeyIUs) {}

   // destructor
   ~GroupBy() {}

   void addCount(const string& name) {
      addAsInt(AggFunction::Count, name);
   }

   void addSum(const string& name, IU* inputIU) {
      addAsType(AggFunction::Sum, name, inputIU);
   }

   void addMax(const string& name, IU* inputIU) {
      addAsType(AggFunction::Max, name, inputIU);
   }

   void addAvg(const string& name, IU* inputIU) {
      addAsType(AggFunction::AvgSum, name, inputIU);
      addAsInt(AggFunction::AvgCnt, name);
   }

   vector<IU*> resultIUs() {
      vector<IU*> v;
      for (auto&[fn, inputIU, resultIU] : aggs)
         v.push_back(&resultIU);      
      return v;
   }

   IUSet inputIUs() {
      IUSet v;
      for (auto&[fn, inputIU, resultIU] : aggs)
         if (inputIU)
            v.add(inputIU);
      return v;
   }

   IUSet availableIUs() override {
      return groupKeyIUs | IUSet(resultIUs());
   }

   void produce(const IUSet& required, ConsumerFn consume) override {
      // build hash table
      print("unordered_map<tuple<{}>, tuple<{}>> {};\n", formatTypes(groupKeyIUs.v), formatTypes(resultIUs()), ht.varname);
      input->produce(groupKeyIUs | inputIUs(), [&]() {
         // insert tuple into hash table
         print("auto it = {}.find({{{}}});\n", ht.varname, formatVarnames(groupKeyIUs.v));
         genBlock(format("if (it == {}.end())", ht.varname), [&]() {
            vector<string> initValues;
            for (auto&[fn, inputIU, resultIU] : aggs) {
               switch (fn) {
                  case (AggFunction::Sum): case (AggFunction::Max):case (AggFunction::AvgSum): {
                     initValues.push_back(inputIU->varname); 
                     break;
                  }
                  case (AggFunction::Count): case (AggFunction::AvgCnt): {
                     initValues.push_back("1"); 
                     break;
                  }
               }
            }
            // insert new group
            print("{}.insert({{{{{}}}, {{{}}}}});\n", ht.varname, formatVarnames(groupKeyIUs.v), fmt::join(initValues, ","));
         });
         genBlock("else", [&]() {
            // update group
            unsigned i=0;
            for (auto&[fn, inputIU, resultIU] : aggs) {
               switch (fn) {
                  case (AggFunction::Sum): case (AggFunction::AvgSum): {
                     print("get<{}>(it->second) += {};\n", i, inputIU->varname); 
                     break;
                  }
                  case (AggFunction::Count): case (AggFunction::AvgCnt): {
                     print("get<{}>(it->second)++;\n", i); 
                     break;
                  }
                  case (AggFunction::Max): {
                     print("get<{0}>(it->second) = max(get<{0}>(it->second), {1});\n", i, inputIU->varname); 
                     break;
                  }
               }
               i++;
            }
         });
      });

      // iterate over hash table
      genBlock(format("for (auto& it : {})", ht.varname), [&]() {
         for (unsigned i=0; i<groupKeyIUs.size(); i++) {
            IU* iu = groupKeyIUs.v[i];
            if (required.contains(iu))
               provideIU(iu, format("get<{}>(it.first)", i));
         }
         unsigned i=0;
         for (auto&[fn, inputIU, resultIU] : aggs) {
            switch (fn) {
               case (AggFunction::Sum): case (AggFunction::Count): case (AggFunction::Max): {
                  provideIU(&resultIU, format("get<{}>(it.second)", i)); 
                  break;
               }
               case (AggFunction::AvgSum): {
                  provideIU(&resultIU, format("get<{}>(it.second) / get<{}>(it.second)", i, i + 1));
                  break;
               }
               case (AggFunction::AvgCnt): break;
            }
            i++;
         }
         consume();
      });
   }

   IU* getIU(const string& attName) {
      for (auto&[fn, inputIU, resultIU] : aggs)
         if (resultIU.name == attName)
            return &resultIU;
      throw;
   }

   private:
      void addAsType(AggFunction af, const string& name, IU* inputIU) {
         aggs.push_back({af, inputIU, {name, inputIU->type}});
      }

      void addAsInt(AggFunction af, const string& name) {
         aggs.push_back({af, nullptr, {name, Type::Integer}});
      }
};

// skyline/pareto operator
struct Pareto : public Operator {
   unique_ptr<Operator> input;
   vector<IU*> compareKeyIUs;
   IU ip{"isPareto", Type::Bool};
   IU v{"vector", Type::Undefined};
   IU pm{"paretoMap", Type::Undefined};
   IU p{"p", Type::Undefined};
   IU q{"q", Type::Undefined};

   // constructor
   Pareto(unique_ptr<Operator> input, const vector<IU*>& compareKeyIUs) : input(std::move(input)), compareKeyIUs(compareKeyIUs)  {}

   // destructor
   ~Pareto() {}

   IUSet availableIUs() override {
      return input->availableIUs();
   }

   void produce(const IUSet& required, ConsumerFn consume) override {
      IUSet requiredIUs = (required & input->availableIUs()) | IUSet(compareKeyIUs);
      vector<IU*> requiredVec = requiredIUs.v;
      string requiredType = format("tuple<{}>", formatTypes(requiredVec));
      string compareType = format("tuple<{}>", formatTypes(compareKeyIUs));
      string requiredVars = formatVarnames(requiredVec);
      string compareVars = formatVarnames(compareKeyIUs);

      // create a vector containing tuples with all required values
      print("\nvector<{}> {};\n", requiredType, v.varname);
      // create a set containing tuples being Pareto points
      // at the beginning set it to all points, then remove non-Pareto
      print("unordered_map<{0}, bool> {1};\n", compareType, pm.varname);

      input->produce(requiredIUs, [&]() {
         // push all required values to the vector
         print("{}.push_back({{{}}});\n", v.varname, requiredVars);
         print("{}[{{{}}}] = true;\n", pm.varname, compareVars);
      });

      genBlock(format("for (auto {0} = {1}.begin(); {0} != {1}.end(); {0}++)", p.varname, pm.varname), [&] {
         genBlock(format("for (auto {0} = {1}.begin(); {0} != {1}.end(); {0}++)", q.varname, pm.varname), [&]{
            // for every keyIU add it to list of required conditions indices
            vector<int> condIndices;
            for (auto it =  compareKeyIUs.begin(); it != compareKeyIUs.end(); it++) {
               condIndices.push_back(getVecIndex(compareKeyIUs, *it));
            }

            // if there is a dominiating q then p is not Pareto
            genBlock(format("if ({})", generateConditions(condIndices, p.varname, q.varname)), [&]{
               print("{}->second = false;\n", p.varname);
               print("break;\n");
            });
         });
      });

      // provide the tuples as requiredIU to the consumer
      genBlock(format("for (auto {}: {})", p.varname, v.varname), [&]{
         for (long unsigned int i = 0; i < requiredVec.size(); i++) {
            provideIU(requiredVec[i], format("get<{}>({})", i, p.varname));
         }

         genBlock(format("if ({}[{{{}}}])", pm.varname, compareVars), [&]{
            consume();
         });
      });
   };

   private:
      string generateConditions(const vector<int>& indices, const string& p, const string& q) {
         stringstream andSs; // dominated if point q is not worse in every dimension
         stringstream orSs; // dominated if point q is strictly better in >=1 dimension

         for (int i: indices) {
            andSs << format("get<{0}>({2}->first) <= get<{0}>({1}->first) &&", i, p, q);
            orSs  << format("get<{0}>({2}->first) < get<{0}>({1}->first) ||",  i, p, q);
         }

         string andStr = andSs.str();
         if (andStr.size() > 3) {
            andStr.erase(andStr.size() - 3); // remove last " &&"
         }

         string orStr = orSs.str();
         if (orStr.size() > 3) {
            orStr.erase(orStr.size() - 3); // remove last " ||"
         }

         return format("({}) && ({})", andStr, orStr);
      }

      // translate a iu to its index in given vector 
      int getVecIndex(const vector<IU*>& v, IU* iu) {
         auto itReq = find(v.begin(), v.end(), iu); 
         assert(itReq != v.end());
         return itReq - v.begin();
      }
};