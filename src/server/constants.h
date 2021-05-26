#pragma once

namespace faas {

constexpr int kConnectionTypeMask           = 0x7fff0000;

// ===== Connection type IDs =====

constexpr int kGatewayIngressTypeId         = 0x00 << 16;
constexpr int kGatewayEgressHubTypeId       = 0x01 << 16;
constexpr int kEngineIngressTypeId          = 0x02 << 16;
constexpr int kEngineEgressHubTypeId        = 0x03 << 16;
constexpr int kSequencerIngressTypeId       = 0x04 << 16;
constexpr int kSequencerEgressHubTypeId     = 0x05 << 16;
constexpr int kStorageIngressTypeId         = 0x06 << 16;
constexpr int kStorageEgressHubTypeId       = 0x07 << 16;

// Timers
constexpr int kTimerTypeId                  = 0x10 << 16;
constexpr int kSLogLocalCutTimerTypeId      = kTimerTypeId + 1;
constexpr int kSLogStateCheckTimerTypeId    = kTimerTypeId + 2;
constexpr int kSendShardProgressTimerId     = kTimerTypeId + 3;
constexpr int kMetaLogCutTimerId            = kTimerTypeId + 3;

// Used by Gateway
constexpr int kHttpConnectionTypeId         = 0x20 << 16;
constexpr int kGrpcConnectionTypeId         = 0x21 << 16;

// Used by Engine
constexpr int kMessageConnectionTypeId      = 0x30 << 16;
constexpr int kSequencerConnectionTypeId    = 0x31 << 16;
constexpr int kIncomingSLogConnectionTypeId = 0x32 << 16;
constexpr int kSLogMessageHubTypeId         = 0x33 << 16;

// ===== Buffer groups =====

// 8-byte buffer group
constexpr uint16_t kOctaBufGroup                   = 0x00;
// Default buffer group for IngressConnection
// Default buffer size is 64KB
constexpr uint16_t kDefaultIngressBufGroup         = 0x01;

// Used by Gateway
constexpr uint16_t kHttpConnectionBufGroup         = 0x10;
constexpr uint16_t kGrpcConnectionBufGroup         = 0x11;

// Used by Engine
constexpr uint16_t kMessageConnectionBufGroup      = 0x20;
constexpr uint16_t kSequencerConnectionBufGroup    = 0x21;
constexpr uint16_t kIncomingSLogConnectionBufGroup = 0x22;

// ===== Journal entry types =====
constexpr uint16_t kMetalogJournalEntryType  = 0x00;
constexpr uint16_t kLogEntryJournalEntryType = 0x01;

}  // namespace faas
