#include "log/core.h"

namespace faas {
namespace log {

Core::Core(uint16_t my_node_id)
    : my_node_id_(my_node_id) {
}

Core::~Core() {
}

void Core::ApplySequencerMessage(std::span<const char> message) {
    
}

const View* Core::GetView(uint16_t view_id) const {
    return nullptr;
}

void Core::NewLocalLog(uint32_t log_tag, std::span<const char> data) {

}

void Core::NewRemoteLog(uint64_t log_localid, uint32_t log_tag, std::span<const char> data) {

}

bool Core::ReadLog(uint64_t log_seqnum, std::span<const char>* data) {
    return storage_->Read(log_seqnum, data);
}

std::string Core::BuildLocalCutMessage() {
    return "";
}

}  // namespace log
}  // namespace faas
