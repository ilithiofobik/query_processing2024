#pragma once

#include <cassert>
#include <string>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <string>
#include <p2c/io.hpp>
#include <p2c/tpch.hpp>
#include <p2c/types.hpp>
#include <p2c/foundations.hpp>
#include "local_buffer.hpp"

using namespace std;
using namespace fmt;
using namespace p2c;

struct ParallelOperator {
   virtual IUSet availableIUs() = 0;
   
   // Change this maybe?
   virtual void produce(const IUSet& required, MorselInitFn morsel, ConsumerFn consume) = 0;

   virtual ~ParallelOperator() {}
};

// Synchronized print
void produceAndSynchronizedPrint(unique_ptr<ParallelOperator> root, const std::vector<IU*>& ius) {
   IU mutex{"mutex", Type::Undefined};
   IU lock{"lock_guard", Type::Undefined};
   print("mutex {};\n", mutex.varname);
   root->produce(IUSet(ius),
      [&]() {},
      [&]() {
         print("lock_guard<mutex> {}({});", lock.varname, mutex.varname);
         for (IU *iu : ius)
            print("cerr << {} << \" \";", iu->varname);
         print("cerr << endl;\n");
      });
}

struct ParallelScan : public ParallelOperator {
   // IU storage for all available attributes
   vector<IU> attributes;
   // relation name
   string relName;

   // constructor
   ParallelScan(const string& relName) : relName(relName) {
      // get relation info from schema
      auto it = TPCH::schema.find(relName);
      assert(it != TPCH::schema.end());
      auto& rel = it->second;
      // create IUs for all available attributes
      attributes.reserve(rel.size());
      for (auto& att : rel)
         attributes.emplace_back(IU{att.first, att.second});
   }

   // destructor
   ~ParallelScan() {}

   IUSet availableIUs() override {
      IUSet result;
      for (auto& iu : attributes)
         result.add(&iu);
      return result;
   }

   void produce(const IUSet& required, MorselInitFn morsel, ConsumerFn consume) override {
      genBlock(format("for (uint64_t i = 0; i != db.{}.tupleCount; i++)", relName), [&]() {
         for (IU* iu : required)
            provideIU(iu, format("db.{}.{}[i]", relName, iu->name));
         consume();
      });
   }

   IU* getIU(const string& attName) {
      for (IU& iu : attributes)
         if (iu.name == attName)
            return &iu;
      throw;
   }
};

// selection operator
struct ParallelSelection : public ParallelOperator {
   unique_ptr<ParallelOperator> input;
   unique_ptr<Exp> pred;

   // constructor
   ParallelSelection(unique_ptr<ParallelOperator> input, unique_ptr<Exp> predicate) : input(std::move(input)), pred(std::move(predicate)) {}

   // destructor
   ~ParallelSelection() {}

   IUSet availableIUs() override {
      return input->availableIUs();
   }

   void produce(const IUSet& required, MorselInitFn morsel, ConsumerFn consume) override {
      input->produce(required | pred->iusUsed(), 
         [&]() {},
         [&]() {
            genBlock(format("if ({})", pred->compile()), [&]() {
               consume();
            });
         }
      );
   }
};

// hash join operator
struct ParallelHashJoin : public ParallelOperator {
   unique_ptr<ParallelOperator> left;
   unique_ptr<ParallelOperator> right;
   vector<IU*> leftKeyIUs, rightKeyIUs;
   IU ht{"joinHT", Type::Undefined};

   // constructor
   ParallelHashJoin(unique_ptr<ParallelOperator> left, unique_ptr<ParallelOperator> right, const vector<IU*>& leftKeyIUs, const vector<IU*>& rightKeyIUs) :
      left(std::move(left)), right(std::move(right)), leftKeyIUs(leftKeyIUs), rightKeyIUs(rightKeyIUs) {}

   // destructor
   ~ParallelHashJoin() {}

   IUSet availableIUs() override {
      return left->availableIUs() | right->availableIUs();
   }

   void produce(const IUSet& required, MorselInitFn morsel, ConsumerFn consume) override {
      // figure out where required IUs come from
      IUSet leftRequiredIUs = (required & left->availableIUs()) | IUSet(leftKeyIUs);
      IUSet rightRequiredIUs = (required & right->availableIUs()) | IUSet(rightKeyIUs);
      IUSet leftPayloadIUs = leftRequiredIUs - IUSet(leftKeyIUs); // these we need to store in hash table as payload

      // build hash table
      print("unordered_multimap<tuple<{}>, tuple<{}>> {};\n", formatTypes(leftKeyIUs), formatTypes(leftPayloadIUs.v), ht.varname);
      left->produce(leftRequiredIUs, 
         [&]() {},
         [&]() {
            // insert tuple into hash table
            print("{}.insert({{{{{}}}, {{{}}}}});\n", ht.varname, formatVarnames(leftKeyIUs), formatVarnames(leftPayloadIUs.v));
         }
      );

      // probe hash table
      right->produce(rightRequiredIUs, 
         [&]() {},
         [&]() {
            // iterate over matches
            genBlock(format("for (auto range = {}.equal_range({{{}}}); range.first!=range.second; range.first++)", ht.varname, formatVarnames(rightKeyIUs)),
                     [&]() {
                        // unpack payload
                        unsigned countP = 0;
                        for (IU* iu : leftPayloadIUs)
                           provideIU(iu, format("get<{}>(range.first->second)", countP++));
                        // unpack keys if needed
                        for (unsigned i=0; i<leftKeyIUs.size(); i++) {
                           IU* iu = leftKeyIUs[i];
                           if (required.contains(iu))
                              provideIU(iu, format("get<{}>(range.first->first)", i));
                        }
                        // consume
                        consume();
                     });
         }
      );
   }
};

////////////////////////////////////////////////////////////////////////////////
