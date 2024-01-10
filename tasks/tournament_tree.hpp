#include <cstdlib>
#include <tuple>
#include <unordered_map>

// the whole implementation is based on the paper
// "Hardware Assited Sorting in IBM's DB2 DBMS"

// for now defining node only for uint32_t
// to be extended for more types
// for now, simple conversion to uint32_t is used

#define MAX_NODES 1024  // TODO: magic number, should be changable?

typedef uint32_t o_t;
typedef uint32_t vc_t;
typedef std::tuple<o_t, vc_t> ovc_t;

template <class T>
struct ovc;

template <typename... Args>
struct ovc<tuple<Args...>> {
    inline ovc_t get_ovc(const tuple<Args...> &args1,
                         const tuple<Args...> &args2) const {
        // TODO: is this needed to be wrapped?
        return calc_ovc(args1, args2);
    }

   private:
    template <unsigned I = 0, typename... Tuple>
    constexpr inline static ovc_t calc_ovc(const tuple<Tuple...> &tuple1,
                                           const tuple<Tuple...> &tuple2) {
        if constexpr (I == sizeof...(Args)) {
            throw std::runtime_error("Tuples are equal");
        } else {
            auto val1 = get<I>(tuple1);
            auto val2 = get<I>(tuple2);
            if (val1 != val2) {
                vc_t v = (vc_t)get<I>(tuple1);
                vc_t vc = std::numeric_limits<vc_t>::max() - v;
                return {I + 1, vc};
            } else {
                return calc_ovc<I + 1, Tuple...>(tuple1, tuple2);
            }
        }
    }
};

// TODO: change to template on type T
struct Node {
    bool isNull;  // TODO: change to optional
    uint32_t winner;
    uint32_t loser;
    std::tuple<uint32_t, uint32_t> key;
    std::tuple<uint32_t, uint32_t> winnerKey;
    std::tuple<uint32_t, uint32_t> loserKey;
    std::tuple<uint32_t, uint32_t> ovc;
    uint32_t index;
    uint32_t parent() { return index / 2; }
    uint32_t leftChild() { return index * 2; }
    uint32_t rightChild() { return index * 2 + 1; }
};

// // Assumption: a and b are not equal
// std::tuple<uint32_t, uint32_t> ovc(std::tuple<uint32_t, uint32_t> a,
//                                    std::tuple<uint32_t, uint32_t> b) {
//     if (get<0>(a) != get<0>(b)) {
//         return {1, 1000 - get<0>(b)};
//     } else {
//         return {2, 1000 - get<1>(b)};
//     }
// }

// // TODO: change to class
// // support random input and reading from file
// struct InputStream {
//     int counter = 0;
//     int inputSize = 10000;

//     std::tuple<uint32_t, uint32_t> getNext() {
//         counter++;
//         if (counter < inputSize) {
//             return std::make_tuple(rand() % 1000, rand() % 1000);
//         } else {
//             return std::make_tuple(1000, 1000);  // infinity
//         }
//     }
// };

// // simulating stream
// // TODO: return infinity at the end
// // std::tuple<uint32_t, uint32_t>
// // getNext() {
// //     return std::make_tuple(rand() % 1000, rand() % 1000);
// // }

// // move to output
// void appendToRun(std::tuple<uint32_t, uint32_t>) {}

// std::tuple<uint32_t, uint32_t> minimalKey() { return std::make_tuple(0, 0); }

// // Current assumptions
// // - 1024 nodes
// // node values from 0 to 999
// struct TreeOfLosers {
//     Node nodes[1024];
//     uint32_t mostRecentWinner;
//     InputStream inputStream;

//     // initialization = step 1
//     TreeOfLosers() {
//         inputStream = {0, 10000};

//         for (int i = 0; i < 1024; i++) {
//             nodes[i].winnerKey = inputStream.getNext();
//             nodes[i].winner = i;
//             nodes[i].ovc = ovc(nodes[i].winnerKey, minimalKey());
//             nodes[i].isNull = false;
//         }

//         for (int i = 1023; i >= 0; i--) {
//             uint32_t leftIdx = nodes[i].leftChild();
//             uint32_t rightIdx = nodes[i].rightChild();
//             if (rightIdx < 1024) {
//                 if (!((nodes[leftIdx]).isNull) &&
//                 !((nodes[rightIdx]).isNull)) {
//                     Node left = (nodes[leftIdx]);
//                     Node right = (nodes[rightIdx]);

//                     // case 1
//                     if (left.ovc < right.ovc) {
//                         nodes[i].winnerKey = right.winnerKey;
//                         nodes[i].winner = right.winner;
//                         nodes[i].loserKey = left.winnerKey;
//                         nodes[i].loser = left.winner;
//                         nodes[i].ovc = left.ovc;
//                     }

//                     // case 2
//                     if (left.ovc > right.ovc) {
//                         nodes[i].winnerKey = left.winnerKey;
//                         nodes[i].winner = left.winner;
//                         nodes[i].loserKey = right.winnerKey;
//                         nodes[i].loser = right.winner;
//                         nodes[i].ovc = right.ovc;
//                     }

//                     // case 3
//                     if (left.ovc == right.ovc) {
//                         // case 3.1
//                         if (left.winner < right.winner) {
//                             nodes[i].winnerKey = left.winnerKey;
//                             nodes[i].winner = left.winner;
//                             nodes[i].loserKey = right.winnerKey;
//                             nodes[i].loser = right.winner;
//                             nodes[i].ovc = ovc(right.key, left.key);
//                         }
//                         // case 3.2
//                         else {
//                             nodes[i].winnerKey = right.winnerKey;
//                             nodes[i].winner = right.winner;
//                             nodes[i].loserKey = left.winnerKey;
//                             nodes[i].loser = left.winner;
//                             nodes[i].ovc = ovc(left.key, right.key);
//                         }
//                     }
//                 }
//                 appendToRun(nodes[0].winnerKey);
//                 mostRecentWinner = nodes[0].winner;
//             }
//         }
//     }

//     // step 2 = run formation
//     // step 3 = tree flush
//     // void formRun() {
//     //     do {

//     //     } while (mostRecentWinner.loserKey != std::tuple(1000, 1000));
//     // }
// };