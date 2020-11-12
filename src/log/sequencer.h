#pragma once

#include "log/common.h"

namespace faas {
namespace log {

class Sequencer {
public:
    Sequencer();
    ~Sequencer();

private:
    DISALLOW_COPY_AND_ASSIGN(Sequencer);
};

}  // namespace log
}  // namespace faas
