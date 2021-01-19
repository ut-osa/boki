#pragma once

#ifndef __FAAS_SRC
#error base/init.h cannot be included outside
#endif

#include <vector>
#include <functional>

namespace faas {
namespace base {

void InitMain(int argc, char* argv[],
              std::vector<char*>* positional_args = nullptr);

// When SIGABRT, SIGTERM signals are received, clean-up functions will
// be invoked before exit.
void ChainCleanupFn(std::function<void()> fn);

// When SIGINT signal is received, this function will be invoked.
void SetInterruptHandler(std::function<void()> fn);

}  // namespace base
}  // namespace faas
