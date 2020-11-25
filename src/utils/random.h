#pragma once

#include "base/common.h"

namespace faas {
namespace utils {

// Thread-safe
int GetRandomInt(int a, int b);  // In [a, b)
float GetRandomFloat(float a = 0.0f, float b = 1.0f);  // In [a, b)
double GetRandomDouble(double a = 0.0, double b = 1.0);  // In [a, b)

}  // namespace utils
}  // namespace faas
