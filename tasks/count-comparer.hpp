
// Function to count comparisons for std::priority_queue
template <typename T>
struct compare_count {
    int& count;
    compare_count(int& count) : count(count) {}
    bool operator()(const T& a, const T& b) {
        count++;
        return a > b;  // sorts in ascending order
    }
};
