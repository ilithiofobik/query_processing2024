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
class TreeOfLosers;

// template is general, but used only on tuples
template <typename T>
struct Node {
    uint32_t winner;
    uint32_t loser;
    T winnerKey;
    T loserKey;
    ovc_t nodeOvc;

    Node() {}
};

// TODO: support random input and reading from file
template <typename T>
class InputStream {
   public:
    virtual T getNext() { return {}; };
};

class RandomInputStream : public InputStream<std::tuple<uint32_t, uint32_t>> {
   public:
    RandomInputStream(uint32_t min, uint32_t max, uint32_t size)
        : min_(min), max_(max), counter_(0), size_(size) {
        srand(time(NULL));
    }

    virtual std::tuple<uint32_t, uint32_t> getNext() override {
        if (counter_ < size_) {
            counter_++;
            uint32_t first = rand() % max_ + min_;
            uint32_t second = rand() % max_ + min_;
            return std::make_tuple(first, second);
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

template <typename T>
class OutputStream {
   public:
    void putNext(T value) = 0;
};

template <typename T>
class VectorOutputStream : public OutputStream<T> {
   public:
    VectorOutputStream() {}
    virtual void putNext(T value) override { vec_.push_back(value); }

   private:
    std::vector<T> &vec_;
};

// TODO: to be generalised
std::tuple<uint32_t, uint32_t> minimalKey() { return std::make_tuple(0, 0); }

#define MAX_NODES 1024  // TODO: magic number, should be changable?

template <typename... Args>
// root at node 1
class TreeOfLosers<tuple<Args...>> {
    std::optional<Node<tuple<Args...>>> nodes[MAX_NODES];
    uint32_t mostRecentWinner;

   public:
    // getting node with given index
    Node<tuple<Args...>> getNode(uint32_t idx) {
        if (idx < MAX_NODES && nodes[idx].has_value()) {
            return nodes[idx].value();
        } else {
            printf("Node does not exist\n");
            throw std::runtime_error("Node does not exist");
        }
    }

    TreeOfLosers(InputStream<tuple<Args...>> &is) {
        // set all nodes to null
        for (int i = 0; i < MAX_NODES; i++) {
            nodes[i] = {};
        }

        // for each leaf node
        // assumption: number of nodes is power of 2
        for (int i = MAX_NODES / 2; i < MAX_NODES; i++) {
            auto newLeaf = Node<tuple<Args...>>();
            newLeaf.winnerKey = is.getNext();
            newLeaf.winner = i;
            newLeaf.nodeOvc = calc_ovc(newLeaf.winnerKey, minimalKey());

            nodes[i] = std::make_optional(newLeaf);
        }

        // for each internal node fill up the tree
        for (int i = MAX_NODES / 2 - 1; i > 0; i--) {
            auto leftIdx = getLeftChild(i);
            auto rightIdx = getRightChild(i);

            if (leftIdx.has_value() && rightIdx.has_value()) {
                printf("i: %d\n", i);
                auto left = nodes[leftIdx.value()].value();
                auto right = nodes[rightIdx.value()].value();
                auto newInternal = Node<tuple<Args...>>();

                // case 1
                if (left.nodeOvc < right.nodeOvc) {
                    newInternal.winnerKey = right.winnerKey;
                    newInternal.winner = right.winner;
                    newInternal.loserKey = left.winnerKey;
                    newInternal.loser = left.winner;
                    newInternal.nodeOvc = left.nodeOvc;
                }

                // case 2
                if (left.nodeOvc > right.nodeOvc) {
                    newInternal.winnerKey = left.winnerKey;
                    newInternal.winner = left.winner;
                    newInternal.loserKey = right.winnerKey;
                    newInternal.loser = right.winner;
                    newInternal.nodeOvc = right.nodeOvc;
                }

                // case 3
                if (left.nodeOvc == right.nodeOvc) {
                    // case 3.1
                    // index or key
                    if (left.winnerKey < right.winnerKey) {
                        newInternal.winnerKey = left.winnerKey;
                        newInternal.winner = left.winner;
                        newInternal.loserKey = right.winnerKey;
                        newInternal.loser = right.winner;
                        newInternal.nodeOvc =
                            calc_ovc(right.winnerKey, left.winnerKey);
                    }
                    // case 3.2
                    else {
                        newInternal.winnerKey = right.winnerKey;
                        newInternal.winner = right.winner;
                        newInternal.loserKey = left.winnerKey;
                        newInternal.loser = left.winner;
                        newInternal.nodeOvc =
                            calc_ovc(left.winnerKey, right.winnerKey);
                    }
                }

                nodes[i] = std::make_optional(newInternal);
            }
        }
    }

   private:
    // tree traversing
    std::optional<uint32_t> getLeftChild(uint32_t idx) {
        uint32_t leftIdx = idx * 2;
        if (leftIdx < MAX_NODES && nodes[leftIdx].has_value()) {
            return std::make_optional(leftIdx);
        } else {
            return {};
        }
    }

    std::optional<uint32_t> getRightChild(uint32_t idx) {
        uint32_t rightIdx = idx * 2 + 1;
        if (rightIdx < MAX_NODES && nodes[rightIdx].has_value()) {
            return std::make_optional(rightIdx);
        } else {
            return {};
        }
    }

    std::optional<uint32_t> getParent(uint32_t idx) {
        // special case
        if (idx == 0) {
            return {};
        };

        uint32_t parentIdx = idx / 2;
        if (parentIdx < MAX_NODES && nodes[parentIdx].has_value()) {
            return std::make_optional(parentIdx);
        } else {
            return {};
        }
    }

    // ovc calculations
    template <unsigned I = 0>
    constexpr inline static ovc_t calc_ovc(const tuple<Args...> &tuple1,
                                           const tuple<Args...> &tuple2) {
        if constexpr (I == sizeof...(Args)) {
            printf("Tuples are equal\n");
            throw std::runtime_error("Tuples are equal");
        } else {
            auto val1 = get<I>(tuple1);
            auto val2 = get<I>(tuple2);
            if (val1 != val2 || I == sizeof...(Args) - 1) {
                vc_t v = (vc_t)get<I>(tuple1);
                vc_t vc = std::numeric_limits<vc_t>::max() - v;
                return {I + 1, vc};
            } else {
                return calc_ovc<I + 1>(tuple1, tuple2);
            }
        }
    }
};