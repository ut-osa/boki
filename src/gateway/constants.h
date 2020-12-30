#pragma once

namespace faas {
namespace gateway {

constexpr int kConnectionTypeMask      = 0x7fff0000;

constexpr int kEngineConnectionTypeId  = 0 << 16;
constexpr int kHttpConnectionTypeId    = 1 << 16;
constexpr int kGrpcConnectionTypeId    = 2 << 16;

constexpr uint16_t kEngineConnectionBufGroup = 1;
constexpr uint16_t kHttpConnectionBufGroup   = 2;
constexpr uint16_t kGrpcConnectionBufGroup   = 3;

}  // namespace gateway
}  // namespace faas
