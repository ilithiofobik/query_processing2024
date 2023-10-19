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
* select sum(o_totalprice) as sum_of_total
* from orders
* */
double samplequery(const TPCH& database) {
    double sumOfTotal {0};

    /*
    Using an iterator
    auto& prices = database.orders.o_totalprice;
    for (auto currentPrice = prices.begin(); currentPrice != prices.end(); ++currentPrice) {
        sumOfTotal += *currentPrice;
    }*/

    auto& prices = database.orders.o_totalprice;
    for (uint64_t idx = 0; idx < database.orders.tupleCount; idx++) {
        sumOfTotal += prices[idx];
    }

    return sumOfTotal;
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
    params.setParam("task", "samplequery");
    params.setParam("workload", tpch_scale);
    params.setParam("variant", "none");
    params.setParam("impl", "hyper");

    for (auto i = 0u; i != 10; ++i) {
        double sum_of_prices;
        {
            PerfEventBlock perf(db.orders.tupleCount, params, i == 0);
            sum_of_prices = samplequery(db);
        }
        std::cerr << sum_of_prices << std::endl;
    }

    return 0;
}
