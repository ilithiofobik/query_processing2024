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
std::pair<std::string, double> manual_aggregation(const TPCH& database) {
    // TODO: Implement me!
    return std::make_pair("Viktor Leis", 0);
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
    params.setParam("task", "manualjoin");
    params.setParam("workload", tpch_scale);
    params.setParam("variant", "x > 1000");
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
