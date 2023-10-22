#include <cmath>
#include <fcntl.h>
#include <iostream>
#include <atomic>
#include <mutex> 
#include <unordered_map>
#include <qp/common/perfevent.hpp>
#include <p2c/io.hpp>
#include <p2c/tpch.hpp>
#include <p2c/types.hpp>
#include <tbb/parallel_for.h>
#include <tbb/blocked_range.h>

using namespace p2c;

inline double roundPrice(double a) { return ceil(a * 100.0) / 100.0; }

/**
 * select count(*), sum(o_totalprice - l_discount)
 * from orders o, lineitem l
 * where l_orderkey = o_orderkey
 * and o_totalprice > 1000
 * */
std::pair<int64_t, double> manual_join(unsigned threads, const TPCH& db) {
    std::atomic<int64_t> count{0};
    double sum = 0.0;
    std::mutex mtx; // using mutex to add sum, no fetch_add for atomic<double>

    uint64_t num_of_orders = db.orders.tupleCount;
    uint64_t num_of_lineitem = db.lineitem.tupleCount;

    uint64_t l_grainsize = (num_of_lineitem % threads == 0) ? (num_of_lineitem / threads) : (num_of_lineitem / threads + 1);

    auto range = tbb::blocked_range<size_t>(0, threads);

    std::unordered_map<int64_t, double> totalprice_map;

    // add all totalprices to a local map
    for (uint64_t o = 0; o < num_of_orders; o++) {
        int64_t o_orderkey = db.orders.o_orderkey[o];
        double o_totalprice = db.orders.o_totalprice[o];
        std::pair<int64_t, double> pair = std::make_pair(o_orderkey, o_totalprice);
        totalprice_map.insert(pair);
    }

    tbb::parallel_for(range, [&](const decltype(range)& r) {
        uint64_t l_start = r.begin() * l_grainsize;
        uint64_t l_end = std::min(r.end() * l_grainsize, num_of_lineitem);

        int64_t local_count = 0;
        double local_sum = 0.0;

        // if price > 1000.0 count and add totalprice-discount to the sum
        for (uint64_t l = l_start; l < l_end; l++) {    
            int64_t l_orderkey = db.lineitem.l_orderkey[l];
            double o_totalprice = totalprice_map[l_orderkey];
            if (o_totalprice > 1000.0) {
                local_count += 1;
                local_sum += o_totalprice - db.lineitem.l_discount[l];
            }
        }

        count.fetch_add(local_count);
        
        mtx.lock();
        sum += local_sum;
        mtx.unlock();
    });

    return std::make_pair(count.load(), sum);
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cout << "Usage: " << argv[0] << " [tpch-dir] [threads]" << std::endl;
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
