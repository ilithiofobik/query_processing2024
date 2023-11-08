#pragma once
#include "qp/common/util.hpp"
#include <cstdint>

using namespace qp;

template<typename Data>
struct LocalBuffer {
   LocalBuffer() {}

   ~LocalBuffer() {
   }

   void push_back(Data data) {
   }

   struct iterator {
   };

   inline iterator begin() const { return iterator{0}; }
   inline const iterator end() const { return iterator{0}; }

   uint64_t size() { return 0; }
};



