#pragma once

#include "log/common.h"
#include "log/fsm.h"

namespace faas {
namespace log {

class SequencerCore {
public:
    SequencerCore();
    ~SequencerCore();

private:
    Fsm fsm_;

    DISALLOW_COPY_AND_ASSIGN(SequencerCore);
};

}  // namespace log
}  // namespace faas
