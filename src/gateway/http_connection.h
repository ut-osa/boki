#pragma once

#include "base/common.h"
#include "common/http_status.h"
#include "utils/appendable_buffer.h"
#include "server/io_worker.h"
#include "gateway/func_call_context.h"

#include <http_parser.h>

namespace faas {
namespace gateway {

class Server;

class HttpConnection final : public server::ConnectionBase {
public:
    static constexpr size_t kBufSize = 65536;

    static constexpr const char* kServerString = "FaaS/0.1";
    static constexpr const char* kResponseContentType = "text/plain";

    HttpConnection(Server* server, int connection_id, int sockfd);
    ~HttpConnection();

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

    FuncCallContext func_call_context_;
    http_parser_settings http_parser_settings_;
    http_parser http_parser_;
    int header_field_value_flag_;
    bool keep_recv_data_;

    // For request
    utils::AppendableBuffer url_buffer_;
    utils::AppendableBuffer header_field_buffer_;
    utils::AppendableBuffer header_value_buffer_;
    utils::AppendableBuffer body_buffer_;
    absl::flat_hash_map<std::string, std::string> headers_;

    // For response
    std::string response_header_;

    void StartRecvData();
    void StopRecvData();
    bool OnRecvData(int status, std::span<const char> data);

    void HttpParserOnMessageBegin();
    void HttpParserOnUrl(const char* data, size_t length);
    void HttpParserOnHeaderField(const char* data, size_t length);
    void HttpParserOnHeaderValue(const char* data, size_t length);
    void HttpParserOnHeadersComplete();
    void HttpParserOnBody(const char* data, size_t length);
    void HttpParserOnMessageComplete();

    void HttpParserOnNewHeader();
    void ResetHttpParser();
    void OnNewHttpRequest(std::string_view method, std::string_view path,
                          std::string_view qs = std::string_view{});
    void SendHttpResponse(HttpStatus status, std::span<const char> body = EMPTY_CHAR_SPAN);
    void OnFuncCallFinishedInternal();

    static int HttpParserOnMessageBeginCallback(http_parser* http_parser);
    static int HttpParserOnUrlCallback(http_parser* http_parser, const char* data, size_t length);
    static int HttpParserOnHeaderFieldCallback(http_parser* http_parser, const char* data, size_t length);
    static int HttpParserOnHeaderValueCallback(http_parser* http_parser, const char* data, size_t length);
    static int HttpParserOnHeadersCompleteCallback(http_parser* http_parser);
    static int HttpParserOnBodyCallback(http_parser* http_parser, const char* data, size_t length);
    static int HttpParserOnMessageCompleteCallback(http_parser* http_parser);

    DISALLOW_COPY_AND_ASSIGN(HttpConnection);
};

}  // namespace gateway
}  // namespace faas
