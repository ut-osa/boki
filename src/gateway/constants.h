#pragma once

namespace faas {
namespace gateway {

constexpr int kConnectionTypeMask      = 0x7fff0000;

constexpr int kEngineIngressTypeId     = 1 << 16;
constexpr int kEngineEgressHubTypeId   = 2 << 16;
constexpr int kHttpConnectionTypeId    = 3 << 16;
constexpr int kGrpcConnectionTypeId    = 4 << 16;

constexpr uint16_t kHttpConnectionBufGroup   = 1;
constexpr uint16_t kGrpcConnectionBufGroup   = 2;

}  // namespace gateway
}  // namespace faas
