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

// makes unique, keeps the order
template <typename T>
std::vector<T> toUnique(const std::vector<T>& v) {
    std::unordered_set<T> used;
    std::vector<T> result;

    for (const T& e : v) {
        if (!used.contains(e)) {
            used.insert(e);
            result.push_back(e);
        }
    }

    return result;
}

template <typename T>
std::vector<tuple<uint64_t, T>> enumerate(const std::vector<T>& v) {
    std::vector<tuple<uint64_t, T>> result;
    result.reserve(v.size());

    for (uint64_t i = 0; i < v.size(); i++) {
        result.push_back({i, v[i]});
    }

    return result;
}

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
            genBlock(format("for (uint64_t i = r.begin(); i < r.end(); i++)"),
                     [&]() {
                         for (IU* iu : toUnique(required.v)) {
                             std::string dbValue =
                                 format("db.{}.{}[i]", relName, iu->name);
                             provideIU(iu, dbValue);
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
    // storage / local buffer
    IU st{"storage", Type::Undefined};
    IU st_loc{"storage_local", Type::Undefined};
    // counter of all tuples
    IU c{"counter", Type::Undefined};
    IU c_loc{"counter_local", Type::Undefined};
    // number of all tuples
    IU ts{"total_size", Type::BigInt};
    // hashtable
    IU ht{"ht", Type::Undefined};
    // range iterator
    IU r{"r", Type::Undefined};
    // vector iterator
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
        // these we need to store in hash table as payload
        IUSet leftPayloadIUs = leftRequiredIUs - IUSet(leftKeyIUs);
        string tuplesTypes =
            format("tuple<{}>, tuple<{}>", formatTypes(leftKeyIUs),
                   formatTypes(leftPayloadIUs.v));
        string entryType = format("Entry<{}>", tuplesTypes);
        string vecType = format("std::vector<{}>", entryType);
        string hashtableType = format("Hashtable<{}>", tuplesTypes);

        // create storage for each thread
        print("tbb::enumerable_thread_specific<{}> {};\n", vecType, st.varname);
        // create counter for each thread
        print("tbb::enumerable_thread_specific<int64_t> {};\n", c.varname);

        left->produce(
            leftRequiredIUs,
            [&]() {
                // pass creating local as morsel init
                print("{}& {} = {}.local();\n", vecType, st_loc.varname,
                      st.varname);
                print("int64_t& {} = {}.local();\n", c_loc.varname, c.varname);
            },
            [&]() {
                // add to local
                print("{}.push_back({}({{{}}}, {{{}}}));\n", st_loc.varname,
                      entryType, formatVarnames(leftKeyIUs),
                      formatVarnames(leftPayloadIUs.v));
                // count element
                print("{}++;\n", c_loc.varname);
            });

        // sum up number of all tuples
        provideIU(&ts, format("std::accumulate({0}.begin(), {0}.end(), 0)",
                              c.varname));
        // initialize hashtable
        print("{1} {0} = {1}({2});\n", ht.varname, hashtableType, ts.varname);

        string parallelFor =
            format("tbb::parallel_for({}.range(), [&](auto {})", st.varname,
                   r.varname);

        genBlock(parallelFor, [&]() {
            genBlock(format("for ({}& {}: {})", vecType, v.varname, r.varname),
                     [&]() {
                         genBlock(
                             format("for (uint64_t i = 0; i < {}.size(); i++)",
                                    v.varname),
                             [&] {
                                 // insert each entry to hashtable (lock-free)
                                 print("{}.insert(&{}[i]);\n", ht.varname,
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
                genBlock(format("for (auto p = {}.equal_range({{{}}}); "
                                "p.first != p.second; ++p.first)",
                                ht.varname, formatVarnames(rightKeyIUs)),
                         [&]() {
                             // unpack payload
                             unsigned countP = 0;
                             for (IU* iu : leftPayloadIUs) {
                                 provideIU(
                                     iu, format("get<{}>(p.first.entry->value)",
                                                countP++));
                             }
                             // unpack keys if needed
                             vector<tuple<uint64_t, IU*>> uniqueKeys =
                                 toUnique(enumerate(leftKeyIUs));
                             for (tuple<uint64_t, IU*>& p : uniqueKeys) {
                                 uint64_t i = get<0>(p);
                                 IU* iu = get<1>(p);

                                 if (required.contains(iu)) {
                                     string keyVal = format(
                                         "get<{}>(p.first.entry->key)", i);
                                     provideIU(iu, keyVal);
                                 }
                             }

                             // consume
                             consume();
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
    IU it{"it", Type::Undefined};
    IU v{"v", Type::Undefined};

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

        // heap as a vector for each thread
        print("tbb::enumerable_thread_specific<{}> {};\n", vecType,
              heap.varname);

        input->produce(
            required | IUSet(keyIUs),
            [&]() {
                // read your vector
                print("{}& {} = {}.local();\n", vecType, heap_loc.varname,
                      heap.varname);
                // reserve memory for exactly k elements
                print("{}.reserve({});\n", heap_loc.varname, K);
            },
            [&]() {
                print("{} {} = {{{}}};\n", tupleType, elem.varname,
                      formatVarnames(keyIUs));
                pushToHeap(heap_loc.varname);
            });

        // declare global vector collecting all heaps
        print("{} {};\n", vecType, v.varname);
        print("{}.reserve({});\n", v.varname, K);

        genBlock(
            format("for (const {}& {}: {})", vecType, it.varname, heap.varname),
            [&] {
                genBlock(
                    // same logic for adding to heap as before
                    format("for(const {}& {}: {})", tupleType, elem.varname,
                           it.varname),
                    [&] { pushToHeap(v.varname); });
            });

        // sort the global heap
        print("sort({0}.begin(), {0}.end());\n", v.varname);

        // provide the tuples to the consumer
        genBlock(
            format("for (const {}& {}: {})", tupleType, it.varname, v.varname),
            [&] {
                for (uint64_t i = 0; i < keyIUs.size(); i++) {
                    provideIU(keyIUs[i], format("get<{}>({})", i, it.varname));
                }
                consume();
            });
    }

   private:
    void pushToHeap(std::string& heapName) {
        // if heap is not full just push
        genBlock(format("if ({}.size() < {})", heapName, K), [&] {
            print("{}.push_back({});\n", heapName, elem.varname);
            print("std::push_heap({0}.begin(), {0}.end());\n", heapName);
        });
        // else check if the new element is better than the worst in
        // heap, if so pop the max elem and add new one
        genBlock(
            format("else if ({} < {}.front())", elem.varname, heapName), [&] {
                print("std::pop_heap({0}.begin(), {0}.end());\n", heapName);
                print("{}.back() = {};\n", heapName, elem.varname);
                print("std::push_heap({0}.begin(), {0}.end());\n", heapName);
            });
    }
};
////////////////////////////////////////////////////////////////////////////////
