#pragma once

namespace faas {
namespace engine {

constexpr int kConnectionTypeMask           = 0x7fff0000;

constexpr int kGatewayIngressTypeId         = 0 << 16;
constexpr int kGatewayEgressHubTypeId       = 1 << 16;
constexpr int kMessageConnectionTypeId      = 2 << 16;
constexpr int kSequencerConnectionTypeId    = 3 << 16;
constexpr int kIncomingSLogConnectionTypeId = 4 << 16;
constexpr int kSLogMessageHubTypeId         = 5 << 16;
constexpr int kTimerTypeId                  = 6 << 16;
constexpr int kSLogLocalCutTimerTypeId      = kTimerTypeId + 1;
constexpr int kSLogStateCheckTimerTypeId    = kTimerTypeId + 2;

constexpr uint16_t kMessageConnectionBufGroup      = 1;
constexpr uint16_t kSequencerConnectionBufGroup    = 2;
constexpr uint16_t kIncomingSLogConnectionBufGroup = 3;

}  // namespace engine
}  // namespace faas
