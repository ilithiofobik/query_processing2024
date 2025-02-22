#include <cmath>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <qp/common/perfevent.hpp>
#include <p2c/io.hpp>
#include <p2c/tpch.hpp>
#include <p2c/types.hpp>


using namespace p2c;

inline double roundPrice(double a) { return ceil(a * 100.0) / 100.0; }

/**
* select o_clerk, max(o_totalprice) as max_order
* from orders
* group by o_clerk
* order by max_order desc
* limit 1
* */
std::pair<std::string, double> manual_aggregation(const TPCH& db) {
    // start with the first order as maximum
    auto o_clerk = db.orders.o_clerk[0];
    double max_o_totalprice = db.orders.o_totalprice[0];
    uint64_t num_of_orders = db.orders.tupleCount;

    // iterate over the rest of orders
    for (uint64_t o = 1; o < num_of_orders; o++) {
        auto o_totalprice = db.orders.o_totalprice[o];
        // change if you found bigger total_price
        if (max_o_totalprice < o_totalprice) {
            o_clerk = db.orders.o_clerk[o];
            max_o_totalprice = o_totalprice;
        }
    }

    // have to convert string_view to string
    return std::make_pair(std::string(o_clerk), max_o_totalprice);
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cout << "Usage: " << argv[0] << " [tpch-dir]" << std::endl;
        return 1;
    }
    std::string tpch_folder(argv[1]);

    auto tpch_scale = tpch_folder.substr(tpch_folder.find_last_of('/') + 1);
    TPCH db(tpch_folder);

    BenchmarkParameters params;
    params.setParam("task", "manualaggregation");
    params.setParam("workload", tpch_scale);
    params.setParam("variant", "limit 1");
    params.setParam("impl", "hyper");

    for (auto i = 0u; i != 10; ++i) {
        std::string clerk;
        double max_order;
        {
            PerfEventBlock perf(db.orders.tupleCount, params, i == 0);
            std::tie(clerk, max_order) = manual_aggregation(db);
        }
        std::cerr << clerk << " | " << max_order << std::endl;
    }

    return 0;
}
