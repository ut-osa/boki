#pragma once

#include "log/common.h"
#include "log/view.h"

namespace faas {
namespace log {

class Sequencer {
public:
    Sequencer();
    ~Sequencer();

private:
    View* current_view_;
    std::vector<std::unique_ptr<View>> views_;

    DISALLOW_COPY_AND_ASSIGN(Sequencer);
};

}  // namespace log
}  // namespace faas
