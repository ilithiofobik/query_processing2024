#pragma once
#include <string>
#include <cassert>
#include <stdexcept>

#ifdef NDEBUG
#define ensure(expr) if (!(expr)) throw std::logic_error("Error: " #expr " was not fulfilled (" __FILE__ ":" + std::to_string(__LINE__) + ")")
#else
#define ensure(expr) assert(expr)
#endif
