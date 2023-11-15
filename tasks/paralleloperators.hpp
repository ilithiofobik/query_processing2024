#pragma once

#include <fmt/core.h>
#include <fmt/ranges.h>

#include <cassert>
#include <p2c/foundations.hpp>
#include <p2c/io.hpp>
#include <p2c/tpch.hpp>
#include <p2c/types.hpp>
#include <string>
#include <unordered_set>

#include "local_buffer.hpp"

using namespace std;
using namespace fmt;
using namespace p2c;

// morsel init function
typedef std::function<void(void)> MorselInitFn;

struct ParallelOperator {
    virtual IUSet availableIUs() = 0;

    virtual void produce(const IUSet& required, MorselInitFn morsel,
                         ConsumerFn consume) = 0;

    virtual ~ParallelOperator() {}
};

// Synchronized print
void produceAndSynchronizedPrint(unique_ptr<ParallelOperator> root,
                                 const std::vector<IU*>& ius) {
    IU mutex{"mutex", Type::Undefined};
    IU lock{"lock_guard", Type::Undefined};
    print("mutex {};\n", mutex.varname);
    root->produce(
        IUSet(ius), [&]() {},
        [&]() {
            print("lock_guard<mutex> {}({});", lock.varname, mutex.varname);
            for (IU* iu : ius) print("cerr << {} << \" \";", iu->varname);
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
        for (auto& iu : attributes) result.add(&iu);
        return result;
    }

    void produce(const IUSet& required, MorselInitFn morsel,
                 ConsumerFn consume) override {
        // ===============================================
        // Sets the scale of perf event counters.
        // Don't change this line.
        print("perf.scale += db.{}.tupleCount;", relName);
        // ===============================================

        string parallelFor = format(
            "tbb::parallel_for(tbb::blocked_range<uint64_t>(0, "
            "db.{}.tupleCount),[&](auto r)",
            relName);

        genBlock(parallelFor, [&]() {
            morsel();
            std::unordered_set<std::string> used;
            genBlock(format("for (uint64_t i = r.begin(); i < r.end(); i++)"),
                     [&]() {
                         for (IU* iu : required) {
                             if (!used.contains(iu->varname)) {
                                 provideIU(iu, format("db.{}.{}[i]", relName,
                                                      iu->name));
                                 used.insert(iu->varname);
                             }
                         }
                         consume();
                     });
        });

        // closing parallel for
        print(");");
    }

    IU* getIU(const string& attName) {
        for (IU& iu : attributes)
            if (iu.name == attName) return &iu;
        throw;
    }
};

// selection operator
struct ParallelSelection : public ParallelOperator {
    unique_ptr<ParallelOperator> input;
    unique_ptr<Exp> pred;

    // constructor
    ParallelSelection(unique_ptr<ParallelOperator> input,
                      unique_ptr<Exp> predicate)
        : input(std::move(input)), pred(std::move(predicate)) {}

    // destructor
    ~ParallelSelection() {}

    IUSet availableIUs() override { return input->availableIUs(); }

    void produce(const IUSet& required, MorselInitFn morsel,
                 ConsumerFn consume) override {
        input->produce(required | pred->iusUsed(), morsel, [&]() {
            genBlock(format("if ({})", pred->compile()), [&]() { consume(); });
        });
    }
};

// hash join operator
struct ParallelHashJoin : public ParallelOperator {
    unique_ptr<ParallelOperator> left;
    unique_ptr<ParallelOperator> right;
    vector<IU*> leftKeyIUs, rightKeyIUs;
    IU st{"storage", Type::Undefined};
    IU st_loc{"storage_local", Type::Undefined};
    IU c{"counter", Type::Undefined};
    IU c_loc{"counter_local", Type::Undefined};
    IU ts{"total_size", Type::BigInt};
    IU ht{"ht", Type::Undefined};
    IU r{"r", Type::Undefined};
    IU v{"v", Type::Undefined};

    // constructor
    ParallelHashJoin(unique_ptr<ParallelOperator> left,
                     unique_ptr<ParallelOperator> right,
                     const vector<IU*>& leftKeyIUs,
                     const vector<IU*>& rightKeyIUs)
        : left(std::move(left)),
          right(std::move(right)),
          leftKeyIUs(leftKeyIUs),
          rightKeyIUs(rightKeyIUs) {}

    // destructor
    ~ParallelHashJoin() {}

    IUSet availableIUs() override {
        return left->availableIUs() | right->availableIUs();
    }

    void produce(const IUSet& required, MorselInitFn morsel,
                 ConsumerFn consume) override {
        // figure out where required IUs come from
        IUSet leftRequiredIUs =
            (required & left->availableIUs()) | IUSet(leftKeyIUs);
        IUSet rightRequiredIUs =
            (required & right->availableIUs()) | IUSet(rightKeyIUs);
        IUSet leftPayloadIUs =
            leftRequiredIUs -
            IUSet(
                leftKeyIUs);  // these we need to store in hash table as payload

        // build local storage
        string tuplesTypes =
            format("tuple<{}>, tuple<{}>", formatTypes(leftKeyIUs),
                   formatTypes(leftPayloadIUs.v));
        string entryType = format("Entry<{}>", tuplesTypes);
        string vecType = format("std::vector<{}>", entryType);
        string hashtableType = format("Hashtable<{}>", tuplesTypes);

        print("tbb::enumerable_thread_specific<{}> {};\n", vecType, st.varname);
        print("tbb::enumerable_thread_specific<int64_t> {};\n", c.varname);

        left->produce(
            leftRequiredIUs,
            [&]() {
                print("{}& {} = {}.local();\n", vecType, st_loc.varname,
                      st.varname);
                print("int64_t& {} = {}.local();\n", c_loc.varname, c.varname);
            },
            [&]() {
                print("{}.push_back({}({{{}}}, {{{}}}));\n", st_loc.varname,
                      entryType, formatVarnames(leftKeyIUs),
                      formatVarnames(leftPayloadIUs.v));
                print("{}++;\n", c_loc.varname);
            });

        provideIU(&ts, format("std::accumulate({0}.begin(), {0}.end(), 0)",
                              c.varname));
        print("auto {} = {}({});\n", ht.varname, hashtableType, ts.varname);

        string parallelFor =
            format("tbb::parallel_for({}.range(), [&](auto {})", st.varname,
                   r.varname);

        genBlock(parallelFor, [&]() {
            genBlock(
                format("for (auto {0} = {1}.begin(); {0} != {1}.end(); {0}++)",
                       v.varname, r.varname),
                [&]() {
                    genBlock(
                        format("for (uint64_t i = 0; i < (*{}).size(); i++)",
                               v.varname),
                        [&] {
                            print("{}.insert(&((*{})[i]));\n", ht.varname,
                                  v.varname);
                        });
                });
        });

        // closing parallel for
        print(");");

        // probe hash table
        right->produce(
            rightRequiredIUs, [&]() {},
            [&]() {
                // iterate over matches
                genBlock(
                    format("for (auto p = {}.equal_range({{{}}}); "
                           "p.first.entry != p.second.entry; p.first.entry = "
                           "p.first.entry->next)",
                           ht.varname, formatVarnames(rightKeyIUs)),
                    [&]() {
                        genBlock("if (p.first.entry->key == p.first.key)", [&] {
                            // unpack payload
                            unsigned countP = 0;
                            for (IU* iu : leftPayloadIUs)
                                provideIU(
                                    iu, format("get<{}>(p.first.entry->value)",
                                               countP++));
                            // unpack keys if needed
                            std::unordered_set<string> used;
                            for (unsigned i = 0; i < leftKeyIUs.size(); i++) {
                                IU* iu = leftKeyIUs[i];
                                if (required.contains(iu) &&
                                    !used.contains(iu->varname)) {
                                    provideIU(
                                        iu,
                                        format("get<{}>(p.first.entry->key)",
                                               i));
                                    used.insert(iu->varname);
                                }
                            }
                            // consume
                            consume();
                        });
                    });
            });
    }
};

struct ParallelTopK : public ParallelOperator {
    unique_ptr<ParallelOperator> input;
    vector<IU*> keyIUs;
    const uint64_t K;
    IU heap{"heap", Type::Undefined};
    IU heap_loc{"heap_local", Type::Undefined};
    IU elem{"elem", Type::Undefined};
    // IU c{"counter", Type::Undefined};
    // IU c_loc{"counter_local", Type::Undefined};
    // IU ts{"total_size", Type::BigInt};
    // IU ht{"ht", Type::Undefined};
    // IU r{"r", Type::Undefined};
    // IU v{"v", Type::Undefined};

    // constructor
    ParallelTopK(unique_ptr<ParallelOperator> input, const vector<IU*>& keyIUs,
                 const uint64_t k)
        : input(std::move(input)), keyIUs(keyIUs), K(k) {}

    // destructor
    ~ParallelTopK() {}

    IUSet availableIUs() override { return input->availableIUs(); }

    void produce(const IUSet& required, MorselInitFn morsel,
                 ConsumerFn consume) override {
        string tupleType = format("tuple<{}>", formatTypes(keyIUs));
        string vecType = format("std::vector<{}>", tupleType);

        print("tbb::enumerable_thread_specific<{}> {};\n", vecType,
              heap.varname);

        input->produce(
            required,
            [&]() {
                print("{}& {} = {}.local();\n", vecType, heap_loc.varname,
                      heap.varname);
            },
            [&]() {
                print("{} {} = {{{}}};\n", tupleType, elem.varname,
                      formatVarnames(keyIUs));
                genBlock(format("if ({}.size() < {})", heap_loc.varname, K),
                         [&] {
                             print("{}.push_back({});\n", heap_loc.varname,
                                   elem.varname);
                         });
                genBlock("else", [&] {
                    print("std::pop_heap({0}.begin(), {0}.end());\n",
                          heap_loc.varname);
                    genBlock(format("if ({} < {}.back())", elem.varname,
                                    heap_loc.varname),
                             [&] {
                                 print("{}.pop_back();\n", heap_loc.varname);
                                 print("{}.push_back({});\n", heap_loc.varname,
                                       elem.varname);
                             });
                });

                print("std::push_heap({0}.begin(), {0}.end());\n",
                      heap_loc.varname);

                consume();
            });
    }
};
////////////////////////////////////////////////////////////////////////////////
