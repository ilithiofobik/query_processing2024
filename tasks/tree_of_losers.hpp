#include <cstdlib>
#include <tuple>
#include <unordered_map>

struct Node {
    uint32_t winner;
    uint32_t loser;
    std::tuple<uint32_t, uint32_t> winnerKey;
    std::tuple<uint32_t, uint32_t> loserKey;
    uint32_t ovc;
    uint32_t index;

    uint32_t parent() { return index / 2; }

    uint32_t leftChild() { return index * 2; }

    uint32_t rightChild() { return index * 2 + 1; }
};

uint32_t ovc(std::tuple<uint32_t, uint32_t> a,
             std::tuple<uint32_t, uint32_t> b) {
    // random output currently
    return std::hash<uint32_t>{}(get<0>(a) ^ get<1>(b));
}

// simulating stream
std::tuple<uint32_t, uint32_t> getNext() {
    return std::make_tuple(rand() % 1000, rand() % 1000);
}

std::tuple<uint32_t, uint32_t> minimalKey() { return std::make_tuple(0, 0); }

// Current assumptions
// - 1024 nodes
// node values from 0 to 999
struct TreeOfLosers {
    Node nodes[1024];

    // initialization = step 1
    TreeOfLosers() {
        for (int i = 0; i < 1024; i++) {
            nodes[i].winnerKey = getNext();
            nodes[i].winner = i;
            nodes[i].ovc = ovc(nodes[i].winnerKey, minimalKey());
        }
    }
};