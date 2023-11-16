#include <fcntl.h>
#include <tbb/blocked_range.h>
#include <tbb/concurrent_hash_map.h>
#include <tbb/enumerable_thread_specific.h>
#include <tbb/parallel_for.h>

#include <cassert>
#include <cmath>
#include <iostream>
#include <numeric>
#include <p2c/io.hpp>
#include <p2c/tpch.hpp>
#include <p2c/types.hpp>
#include <qp/common/perfevent.hpp>

#include "hashtable.hpp"

using namespace p2c;

inline double roundPrice(double a) { return ceil(a * 100.0) / 100.0; }

/**
 * select count(*), sum(o_totalprice - l_discount)
 * from orders o, lineitem l
 * where l_orderkey = o_orderkey
 * and o_totalprice > 1000
 * */
std::pair<int64_t, double> manual_join(unsigned threads, const TPCH& db) {
    // for count(*)
    tbb::enumerable_thread_specific<int64_t> count;
    // for sum(o_totalprice - l_discount)
    tbb::enumerable_thread_specific<double> sum;

    uint64_t num_of_orders = db.orders.tupleCount;
    uint64_t num_of_lineitem = db.lineitem.tupleCount;

    // each thread will take a part of orders
    auto o_range =
        tbb::blocked_range<uint64_t>(0, num_of_orders, num_of_orders / threads);
    // and a part of lineitems
    auto l_range = tbb::blocked_range<uint64_t>(0, num_of_lineitem,
                                                num_of_lineitem / threads);

    tbb::enumerable_thread_specific<
        std::vector<Entry<std::tuple<int64_t>, std::tuple<double>>>>
        storage;

    tbb::enumerable_thread_specific<int64_t> counter;

    tbb::parallel_for(o_range, [&](auto r) {
        std::vector<Entry<std::tuple<int64_t>, std::tuple<double>>>&
            storage_local = storage.local();
        int64_t& counter_local = counter.local();

        for (uint64_t o = r.begin(); o < r.end(); o++) {
            storage_local.push_back(
                Entry<std::tuple<int64_t>, std::tuple<double>>(
                    {db.orders.o_orderkey[o]}, {db.orders.o_totalprice[o]}));
            counter_local++;
        }
    });

    int64_t total_size = std::accumulate(counter.begin(), counter.end(), 0);
    auto ht = Hashtable<std::tuple<int64_t>, std::tuple<double>>(total_size);

    tbb::parallel_for(storage.range(), [&](auto r) {
        for (auto v = r.begin(); v != r.end(); v++) {
            for (uint64_t i = 0; i < (*v).size(); i++) {
                ht.insert(&((*v)[i]));
            }
        }
    });

    tbb::parallel_for(l_range, [&](auto r) {
        int64_t& local_count = count.local();
        double& local_sum = sum.local();

        // iterate over lineitems for given thread
        for (uint64_t l = r.begin(); l < r.end(); l++) {
            int64_t l_orderkey = db.lineitem.l_orderkey[l];

            for (auto p = ht.equal_range({l_orderkey}); p.first != p.second;
                 ++p.first) {
                double o_totalprice = get<0>(p.first.entry->value);
                if (o_totalprice > 1000.0) {
                    local_count++;
                    local_sum += o_totalprice - db.lineitem.l_discount[l];
                }
            }
        }
    });

    int64_t count_out = std::accumulate(count.begin(), count.end(), 0);
    double sum_out = std::accumulate(sum.begin(), sum.end(), 0.0);

    return std::make_pair(count_out, sum_out);
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cout << "Usage: " << argv[0] << " [tpch-dir] [threads]"
                  << std::endl;
        return 1;
    }
    std::string tpch_folder(argv[1]);
    auto tpch_scale = tpch_folder.substr(tpch_folder.find_last_of('/') + 1);
    unsigned threads = atoi(argv[2]);

    TPCH db(tpch_folder);

    BenchmarkParameters params;
    params.setParam("task", "manualjoin");
    params.setParam("workload", tpch_scale);
    params.setParam("variant", "x > 1000");
    params.setParam("impl", "hyper");

    for (auto i = 0u; i != 10; ++i) {
        int64_t count;
        double sum;
        {
            PerfEventBlock perf(db.orders.tupleCount, params, i == 0);
            std::tie(count, sum) = manual_join(threads, db);
        }
        std::cerr << count << " | " << sum << std::endl;
    }

    return 0;
}
