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

   // constructor
   Sort(unique_ptr<Operator> input, const vector<IU*>& keyIUs, bool desc = false) : input(std::move(input)), keyIUs(keyIUs), desc(desc)  {}

   // destructor
   ~Sort() {}

   IUSet availableIUs() override {
      return input->availableIUs();
   }

   void produce(const IUSet& required, ConsumerFn consume) override {
      throw std::logic_error("Implement me!");
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
                  i++; // ignore AvgCnt
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

   // constructor
   Pareto(unique_ptr<Operator> input, const vector<IU*>& compareKeyIUs) : input(std::move(input)), compareKeyIUs(compareKeyIUs)  {}

   // destructor
   ~Pareto() {}

   IUSet availableIUs() override {
      return input->availableIUs();
   }

   void produce(const IUSet& required, ConsumerFn consume) override {
      throw std::logic_error("Implement me!");
   };
};