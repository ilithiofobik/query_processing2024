#include <cstdint>
#include <numeric>
#include <oneapi/tbb.h>
#include <iostream>
#include <oneapi/tbb/blocked_range.h>
#include <oneapi/tbb/enumerable_thread_specific.h>
#include <oneapi/tbb/parallel_for.h>

uint64_t sumSingle(uint64_t n) {
   // single threaded
   uint64_t sum = 0;
   for (uint64_t i = 1; i <= n; i++) {
      sum += i;
   }
   return sum;
}
uint64_t sumThisIsIncorrect(uint64_t n) {
   uint64_t sum = 0;
   tbb::parallel_for(tbb::blocked_range<uint64_t>(1, n+1), [&sum](auto range){
      for (uint64_t i = range.begin(); i < range.end(); i++) {
         sum += i; // this is incorrect
      }
   });
   return sum;
}
uint64_t sumAtomic(uint64_t n) {
   // global atomic
   std::atomic<uint64_t> sum = 0;
   tbb::parallel_for(tbb::blocked_range<uint64_t>(1, n+1), [&sum](auto range){
      for (uint64_t i = range.begin(); i < range.end(); i++) {
         sum += i;
      }
   });
   return sum;
}
uint64_t sumAtomic2(uint64_t n) {
   std::atomic<uint64_t> sum = 0;
   tbb::parallel_for(tbb::blocked_range<uint64_t>(1, n+1), [&sum](auto range){
      // global atomic with local sum
      uint64_t local_sum = 0;
      for (uint64_t i = range.begin(); i < range.end(); i++) {
         local_sum += i;
      }
      sum += local_sum;
   });
   return sum;
}
uint64_t sumTbb(uint64_t n) {
   // tbb thread local
   tbb::enumerable_thread_specific<uint64_t> sum = 0;
   tbb::parallel_for(tbb::blocked_range<uint64_t>(1, n+1), [&sum](auto range){
      uint64_t& local_sum = sum.local();
      for (uint64_t i = range.begin(); i < range.end(); i++) {
         local_sum += i;
      }
   });
   return std::accumulate(sum.begin(), sum.end(), 0);
}

int main() {
   std::cout << sumThisIsIncorrect(1e9) << std::endl;
   //std::cout << sumSingle(1e9) << std::endl;
   //std::cout << sumAtomic2(1e9) << std::endl;
   //std::cout << sumTbb(1e9) << std::endl;
}
