#pragma once

#include "log/common.h"
#include "log/fsm.h"

#include <google/protobuf/arena.h>

namespace faas {
namespace log {

class SequencerCore {
public:
    SequencerCore();
    ~SequencerCore();

    typedef std::function<void(uint16_t /* node_id */, std::span<const char> /* data */)>
            SendFsmRecordsMessageCallback;
    void SetSendFsmRecordsMessageCallback(SendFsmRecordsMessageCallback cb);

    void OnNewNodeConnected(uint16_t node_id, std::string_view addr);
    void OnNodeDisconnected(uint16_t node_id);

    void NewLocalCutMessage(const LocalCutMsgProto& message);

private:
    Fsm fsm_;
    std::vector<FsmRecordProto*> fsm_records_;
    google::protobuf::Arena protobuf_arena_;  // Used for FsmRecordProto

    SendFsmRecordsMessageCallback send_fsm_records_message_cb_;

    absl::flat_hash_map</* node_id */ uint16_t, /* addr */ std::string>
        conencted_nodes_;

    std::vector<uint32_t> local_cuts_;

    void NewView();
    void NewGlobalCut();
    void SendAllFsmRecords(uint16_t node_id);
    void BroadcastFsmRecord(const Fsm::View* view, const FsmRecordProto& record);

    DISALLOW_COPY_AND_ASSIGN(SequencerCore);
};

}  // namespace log
}  // namespace faas
