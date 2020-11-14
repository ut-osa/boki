#pragma once

#include "log/common.h"

namespace faas {
namespace log {

class SequencerCore {
public:
    SequencerCore();
    ~SequencerCore();

private:
    DISALLOW_COPY_AND_ASSIGN(SequencerCore);
};

}  // namespace log
}  // namespace faas
