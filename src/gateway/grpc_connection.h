#pragma once

#include "base/common.h"
#include "utils/appendable_buffer.h"
#include "utils/object_pool.h"
#include "server/types.h"
#include "gateway/func_call_context.h"

__BEGIN_THIRD_PARTY_HEADERS
#include <nghttp2/nghttp2.h>
__END_THIRD_PARTY_HEADERS

namespace faas {
namespace gateway {

class Server;

class GrpcConnection final : public server::ConnectionBase {
public:
    static constexpr size_t kBufSize = 65536;

    static constexpr size_t kH2FrameHeaderByteSize = 9;
    static constexpr size_t kGrpcLPMPrefixByteSize = 5;

    GrpcConnection(Server* server, int connection_id, int sockfd);
    ~GrpcConnection();

    void Start(server::IOWorker* io_worker) override;
    void ScheduleClose() override;

    void OnFuncCallFinished(FuncCallContext* func_call_context);

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    Server* server_;
    State state_;
    int sockfd_;
    server::IOWorker* io_worker_;

    std::string log_header_;

    nghttp2_session* h2_session_;
    nghttp2_error_code h2_error_code_;
    bool mem_send_ongoing_;

    struct H2StreamContext;
    utils::SimpleObjectPool<H2StreamContext> h2_stream_context_pool_;
    utils::SimpleObjectPool<FuncCallContext> func_call_contexts_;
    absl::flat_hash_map</* stream_id */ int32_t, FuncCallContext*> grpc_calls_;

    bool OnRecvData(int status, std::span<const char> data);

    H2StreamContext* H2NewStreamContext(int stream_id);
    H2StreamContext* H2GetStreamContext(int stream_id);
    void H2ReclaimStreamContext(H2StreamContext* stream_context);

    void H2TerminateWithError(nghttp2_error_code error_code);
    bool H2SessionTerminated();
    void H2SendPendingDataIfNecessary();
    void H2SendSettingsFrame();
    bool H2ValidateAndPopulateHeader(H2StreamContext* stream_context,
                                     std::string_view name, std::string_view value);
    void H2SendResponse(H2StreamContext* stream_context);
    bool H2HasTrailersToSend(H2StreamContext* stream_context);
    void H2SendTrailers(H2StreamContext* stream_context);

    void OnNewGrpcCall(H2StreamContext* stream_context);
    void OnFuncCallFinishedInternal(int32_t stream_id);

    int H2OnFrameRecv(const nghttp2_frame* frame);
    int H2OnStreamClose(int32_t stream_id, uint32_t error_code);
    int H2OnHeader(const nghttp2_frame* frame, std::string_view name,
                   std::string_view value, uint8_t flags);
    int H2OnBeginHeaders(const nghttp2_frame* frame);
    int H2OnDataChunkRecv(uint8_t flags, int32_t stream_id, const uint8_t* data, size_t len);
    ssize_t H2DataSourceRead(H2StreamContext* stream_context,
                             uint8_t* buf, size_t length, uint32_t* data_flags);
    int H2SendData(H2StreamContext* stream_context, nghttp2_frame* frame,
                   const uint8_t* framehd, size_t length);

    static int H2ErrorCallback(nghttp2_session* session, int lib_error_code, const char* msg,
                               size_t len, void* user_data);
    static int H2OnFrameRecvCallback(nghttp2_session* session, const nghttp2_frame* frame,
                                     void* user_data);
    static int H2OnStreamCloseCallback(nghttp2_session* session, int32_t stream_id,
                                       uint32_t error_code, void* user_data);
    static int H2OnHeaderCallback(nghttp2_session* session, const nghttp2_frame* frame,
                                  const uint8_t* name, size_t namelen,
                                  const uint8_t* value, size_t valuelen,
                                  uint8_t flags, void* user_data);
    static int H2OnBeginHeadersCallback(nghttp2_session* session, const nghttp2_frame* frame,
                                        void* user_data);
    static int H2OnDataChunkRecvCallback(nghttp2_session* session, uint8_t flags, int32_t stream_id,
                                         const uint8_t* data, size_t len, void* user_data);
    static ssize_t H2DataSourceReadCallback(nghttp2_session* session, int32_t stream_id,
                                            uint8_t* buf, size_t length, uint32_t* data_flags,
                                            nghttp2_data_source* source, void* user_data);
    static int H2SendDataCallback(nghttp2_session* session, nghttp2_frame* frame,
                                  const uint8_t* framehd, size_t length,
                                  nghttp2_data_source* source, void* user_data);

    DISALLOW_COPY_AND_ASSIGN(GrpcConnection);
};

}  // namespace gateway
}  // namespace faas
