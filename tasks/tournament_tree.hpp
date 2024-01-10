#include <cstdlib>
#include <optional>
#include <tuple>
#include <unordered_map>

// the whole implementation is based on the paper
// "Hardware Assited Sorting in IBM's DB2 DBMS"

// for now defining node only for uint32_t
// to be extended for more types
// for now, simple conversion to uint32_t is used

typedef uint32_t o_t;
typedef uint32_t vc_t;
typedef std::tuple<o_t, vc_t> ovc_t;

template <class T>
struct ovc;

template <class T>
class TreeOfLosers;

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
template <typename T>
struct Node {
    bool isNull;  // TODO: change to optional
    uint32_t winner;
    uint32_t loser;
    T key;
    T winnerKey;
    T loserKey;
    ovc_t nodeOvc;
    uint32_t index;
    uint32_t parent() { return index / 2; }
    uint32_t leftChild() { return index * 2; }
    uint32_t rightChild() { return index * 2 + 1; }
};

// TODO: change to class
// support random input and reading from file
template <typename T>
class InputStream {
   public:
    InputStream() {}
    virtual T getNext();
};

class RandomInputStream : public InputStream<std::tuple<uint32_t, uint32_t>> {
   public:
    RandomInputStream(uint32_t min, uint32_t max, uint32_t size)
        : min_(min), max_(max), counter_(0), size_(size) {
        srand(time(NULL));
    }

    std::tuple<uint32_t, uint32_t> getNext() {
        if (counter_ < size_) {
            counter_++;
            return std::make_tuple(rand() % max_ + min_, rand() % max_ + min_);
        } else {
            return std::make_tuple(max_, max_);  // return infinity
        }
    }

   private:
    uint32_t min_;
    uint32_t max_;
    uint32_t counter_;
    uint32_t size_;
};

std::tuple<uint32_t, uint32_t> minimalKey() { return std::make_tuple(0, 0); }

#define MAX_NODES 1024  // TODO: magic number, should be changable?

template <typename... Args>
class TreeOfLosers<tuple<Args...>> {
    std::optional<Node<tuple<Args...>>> nodes[MAX_NODES];
    uint32_t mostRecentWinner;
    InputStream<tuple<Args...>> inputStream;

    TreeOfLosers(InputStream<tuple<Args...>> inputStream)
        : inputStream(inputStream) {}

    void initialize() {
        for (int i = 0; i < 1024; i++) {
            nodes[i].winnerKey = inputStream.getNext();
            nodes[i].winner = i;
            nodes[i].ovc = ovc(nodes[i].winnerKey, minimalKey());
            nodes[i].isNull = false;
        }
    }

    // initialization = step 1
    TreeOfLosers() {
        inputStream = {0, 10000};

        for (int i = 0; i < 1024; i++) {
            nodes[i].winnerKey = inputStream.getNext();
            nodes[i].winner = i;
            nodes[i].ovc = ovc(nodes[i].winnerKey, minimalKey());
            nodes[i].isNull = false;
        }

        for (int i = 1023; i >= 0; i--) {
            uint32_t leftIdx = nodes[i].leftChild();
            uint32_t rightIdx = nodes[i].rightChild();
            if (rightIdx < 1024) {
                if (!((nodes[leftIdx]).isNull) && !((nodes[rightIdx]).isNull)) {
                    auto left = (nodes[leftIdx]);
                    auto right = (nodes[rightIdx]);

                    // case 1
                    if (left.nodeOvc < right.nodeOvc) {
                        nodes[i].winnerKey = right.winnerKey;
                        nodes[i].winner = right.winner;
                        nodes[i].loserKey = left.winnerKey;
                        nodes[i].loser = left.winner;
                        nodes[i].nodeOvc = left.nodeOvc;
                    }

                    // case 2
                    if (left.nodeOvc > right.nodeOvc) {
                        nodes[i].winnerKey = left.winnerKey;
                        nodes[i].winner = left.winner;
                        nodes[i].loserKey = right.winnerKey;
                        nodes[i].loser = right.winner;
                        nodes[i].nodeOvc = right.nodeOvc;
                    }

                    // case 3
                    if (left.ovc == right.ovc) {
                        // case 3.1
                        if (left.winner < right.winner) {
                            nodes[i].winnerKey = left.winnerKey;
                            nodes[i].winner = left.winner;
                            nodes[i].loserKey = right.winnerKey;
                            nodes[i].loser = right.winner;
                            nodes[i].ovc = ovc(right.key, left.key);
                        }
                        // case 3.2
                        else {
                            nodes[i].winnerKey = right.winnerKey;
                            nodes[i].winner = right.winner;
                            nodes[i].loserKey = left.winnerKey;
                            nodes[i].loser = left.winner;
                            nodes[i].ovc = ovc(left.key, right.key);
                        }
                    }
                }
                appendToRun(nodes[0].winnerKey);
                mostRecentWinner = nodes[0].winner;
            }
        }
    }

    // step 2 = run formation
    // step 3 = tree flush
    // void formRun() {
    //     do {

    //     } while (mostRecentWinner.loserKey != std::tuple(1000, 1000));
    // }
};