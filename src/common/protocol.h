#pragma once

#include "base/common.h"
#include "common/time.h"
#include "utils/bits.h"

namespace faas {
namespace protocol {

constexpr int kFuncIdBits   = 8;
constexpr int kMethodIdBits = 6;
constexpr int kClientIdBits = 14;

constexpr int kMaxFuncId   = (1 << kFuncIdBits) - 1;
constexpr int kMaxMethodId = (1 << kMethodIdBits) - 1;
constexpr int kMaxClientId = (1 << kClientIdBits) - 1;

union FuncCall {
    struct {
        uint16_t func_id   : 8;
        uint16_t method_id : 6;
        uint16_t client_id : 14;
        uint32_t call_id   : 32;
        uint16_t padding   : 4;
    } __attribute__ ((packed));
    uint64_t full_call_id;
};
static_assert(sizeof(FuncCall) == 8, "Unexpected FuncCall size");

constexpr FuncCall kInvalidFuncCall = { .full_call_id = 0 };
constexpr uint64_t kInvalidFuncCallId = 0;

#define NEW_EMPTY_FUNC_CALL(FC_VAR) \
    FuncCall FC_VAR; FC_VAR.full_call_id = 0

class FuncCallHelper {
public:
    static FuncCall New(uint16_t func_id, uint16_t client_id, uint32_t call_id) {
        NEW_EMPTY_FUNC_CALL(func_call);
        func_call.func_id = func_id;
        func_call.client_id = client_id;
        func_call.call_id = call_id;
        return func_call;
    }

    static FuncCall NewWithMethod(uint16_t func_id, uint16_t method_id,
                                  uint16_t client_id, uint32_t call_id) {
        NEW_EMPTY_FUNC_CALL(func_call);
        func_call.func_id = func_id;
        func_call.method_id = method_id;
        func_call.client_id = client_id;
        func_call.call_id = call_id;
        return func_call;
    }

    static std::string DebugString(const FuncCall& func_call) {
        if (func_call.method_id == 0) {
            return fmt::format("func_id={}, client_id={}, call_id={}",
                                func_call.func_id, func_call.client_id, func_call.call_id);
        } else {
            return fmt::format("func_id={}, method_id={}, client_id={}, call_id={}",
                               func_call.func_id, func_call.method_id,
                               func_call.client_id, func_call.call_id);
        }
    }

private:
    DISALLOW_IMPLICIT_CONSTRUCTORS(FuncCallHelper);
};

#undef NEW_EMPTY_FUNC_CALL

enum class MessageType : uint16_t {
    INVALID               = 0,
    ENGINE_HANDSHAKE      = 1,
    LAUNCHER_HANDSHAKE    = 2,
    FUNC_WORKER_HANDSHAKE = 3,
    HANDSHAKE_RESPONSE    = 4,
    CREATE_FUNC_WORKER    = 5,
    INVOKE_FUNC           = 6,
    DISPATCH_FUNC_CALL    = 7,
    FUNC_CALL_COMPLETE    = 8,
    FUNC_CALL_FAILED      = 9,
    SHARED_LOG_OP         = 10
};

enum class SharedLogOpType : uint16_t {
    INVALID     = 0x00,
    APPEND      = 0x01,  // FuncWorker to Engine
    READ_NEXT   = 0x02,  // FuncWorker to Engine, Engine to Index
    READ_PREV   = 0x03,  // FuncWorker to Engine, Engine to Index
    TRIM        = 0x04,  // FuncWorker to Engine, Engine to Sequencer
    SET_AUXDATA = 0x05,  // FuncWorker to Engine, Engine to Storage
    READ_NEXT_B = 0x06,  // FuncWorker to Engine, Engine to Index
    READ_AT     = 0x10,  // Index to Storage
    REPLICATE   = 0x11,  // Engine to Storage
    INDEX_DATA  = 0x12,  // Engine to Index
    SHARD_PROG  = 0x13,  // Storage to Sequencer
    METALOGS    = 0x14,  // Sequencer to Sequencer, Engine, Storage, Index
    META_PROG   = 0x15,  // Sequencer to Sequencer
    RESPONSE    = 0x20
};

enum class SharedLogResultType : uint16_t {
    INVALID     = 0x00,
    // Successful results
    APPEND_OK   = 0x20,
    READ_OK     = 0x21,
    TRIM_OK     = 0x22,
    LOCALID     = 0x23,
    AUXDATA_OK  = 0x24,
    // Error results
    BAD_ARGS    = 0x30,
    DISCARDED   = 0x31,  // Log to append is discarded
    EMPTY       = 0x32,  // Cannot find log entries satisfying requirements
    DATA_LOST   = 0x33,  // Failed to extract log data
    TRIM_FAILED = 0x34
};

constexpr uint64_t kInvalidLogTag     = std::numeric_limits<uint64_t>::max();
constexpr uint64_t kInvalidLogLocalId = std::numeric_limits<uint64_t>::max();
constexpr uint64_t kInvalidLogSeqNum  = std::numeric_limits<uint64_t>::max();
constexpr uint64_t kInvalidAuxBufId   = std::numeric_limits<uint64_t>::max();

constexpr uint32_t kFuncWorkerUseEngineSocketFlag = (1 << 0);
constexpr uint32_t kUseFifoForNestedCallFlag      = (1 << 1);
constexpr uint32_t kAsyncInvokeFuncFlag           = (1 << 2);
constexpr uint32_t kUseAuxBufferFlag              = (1 << 3);

struct Message {
    struct {
        uint16_t message_type : 4;
        uint16_t func_id      : 8;
        uint16_t method_id    : 6;
        uint16_t client_id    : 14;
        uint32_t call_id;
    } __attribute__ ((packed));
    union {
        uint64_t parent_call_id;      // [8:16]  Used in INVOKE_FUNC, saved as full_call_id
        struct {
            int32_t dispatch_delay;   // [8:12]  Used in FUNC_CALL_COMPLETE, FUNC_CALL_FAILED
            int32_t processing_time;  // [12:16] Used in FUNC_CALL_COMPLETE
        } __attribute__ ((packed));
        uint64_t log_seqnum;          // [8:16]  Used in SHARED_LOG_OP
        uint64_t log_localid;         // [8:16]  Used in SHARED_LOG_OP
    };
    int64_t send_timestamp;       // [16:24]
    int32_t payload_size;         // [24:28] Used in HANDSHAKE_RESPONSE, INVOKE_FUNC,
                                  //                 FUNC_CALL_COMPLETE, SHARED_LOG_OP
    uint32_t flags;               // [28:32]

    struct {
        uint16_t log_op;          // [32:34]
        union {                   // [34:36]
            uint16_t log_result;
            uint16_t log_client_id;
            uint16_t engine_id;
        };
    } __attribute__ ((packed));

    uint16_t log_num_tags;        // [36:38]
    uint16_t log_aux_data_size;   // [38:40]

    uint64_t log_tag;             // [40:48]
    uint64_t log_client_data;     // [48:56] will be preserved for response to clients

    uint64_t _8_padding_8_;

    char inline_data[__FAAS_MESSAGE_SIZE - __FAAS_CACHE_LINE_SIZE]
        __attribute__ ((aligned (__FAAS_CACHE_LINE_SIZE)));
};

#define MESSAGE_HEADER_SIZE      __FAAS_CACHE_LINE_SIZE
#define MESSAGE_INLINE_DATA_SIZE (__FAAS_MESSAGE_SIZE - MESSAGE_HEADER_SIZE)
static_assert(sizeof(Message) == __FAAS_MESSAGE_SIZE, "Unexpected Message size");

enum class ConnType : uint16_t {
    GATEWAY_TO_ENGINE      = 0,
    ENGINE_TO_GATEWAY      = 1,
    SLOG_ENGINE_TO_ENGINE  = 2,   // Index
    ENGINE_TO_SEQUENCER    = 3,   // Trim
    SEQUENCER_TO_ENGINE    = 4,   // Meta log propagation
    SEQUENCER_TO_SEQUENCER = 5,   // Meta log progress
    ENGINE_TO_STORAGE      = 6,   // Replicate, aux data
    STORAGE_TO_ENGINE      = 7,   // Read result
    SEQUENCER_TO_STORAGE   = 8,   // Meta log
    STORAGE_TO_SEQUENCER   = 9    // Meta log propagation
};

struct HandshakeMessage {
    uint16_t conn_type;
    uint16_t src_node_id;
} __attribute__ ((packed));

static_assert(sizeof(HandshakeMessage) == 4, "Unexpected HandshakeMessage size");

inline std::string EncodeHandshakeMessage(ConnType type, uint16_t src_node_id = 0) {
    HandshakeMessage message = {
        .conn_type = static_cast<uint16_t>(type),
        .src_node_id = src_node_id, 
    };
    return std::string(reinterpret_cast<const char*>(&message),
                       sizeof(HandshakeMessage));
}

struct AuxBufferHeader {
    uint64_t id;
    uint64_t size;
} __attribute__ ((packed));

static_assert(sizeof(AuxBufferHeader) == 16, "Unexpected AuxBufferHeader size");

inline std::string EncodeAuxBufferHeader(uint64_t id, size_t size) {
    AuxBufferHeader message = {
        .id = id,
        .size = static_cast<uint64_t>(size), 
    };
    return std::string(reinterpret_cast<const char*>(&message),
                       sizeof(AuxBufferHeader));
}

struct GatewayMessage {
    struct {
        uint16_t message_type : 4;
        uint16_t func_id      : 8;
        uint16_t method_id    : 6;
        uint16_t client_id    : 14;
        uint32_t call_id;
    } __attribute__ ((packed));
    union {
        int32_t  processing_time; // Used in FUNC_CALL_COMPLETE
        int32_t  status_code;     // Used in FUNC_CALL_FAILED
        uint32_t logspace;        // Used in DISPATCH_FUNC_CALL
    };
    uint32_t payload_size;        // Used in DISPATCH_FUNC_CALL, FUNC_CALL_COMPLETE
} __attribute__ ((packed));

static_assert(sizeof(GatewayMessage) == 16, "Unexpected GatewayMessage size");

constexpr uint16_t kReadInitialFlag = (1 << 0);

struct SharedLogMessage {
    uint16_t op_type;         // [0:2]
    union {                   // [2:4]
        uint16_t op_result;
        uint16_t flags;
    };

    uint16_t origin_node_id;  // [4:6]
    uint16_t hop_times;       // [6:8]
    uint32_t payload_size;    // [8:12]

    union {
        struct {
            uint16_t sequencer_id;   // [12:14]
            uint16_t view_id;        // [14:16]
        } __attribute__ ((packed));
        uint32_t logspace_id;        // [12:16]
    };

    union {
        uint32_t metalog_position; // [16:20] (only used by META_PROG)
        uint32_t user_logspace;    // [16:20]
    };

    union {
        uint32_t seqnum_lowhalf;  // [20:24] (the high half is logspace_id)
        struct {
            uint16_t prev_view_id;
            uint16_t prev_engine_id;
        } __attribute__ ((packed));
    };
    union {
        uint64_t query_tag;   // [24:32]
        struct {
            uint16_t num_tags;      // [24:26]
            uint16_t aux_data_size; // [26:28]

            uint32_t _5_padding_5_;
        } __attribute__ ((packed));
    };

    uint64_t user_metalog_progress;  // [32:40]

    union {
        uint64_t localid;       // [40:48]
        uint64_t query_seqnum;  // [40:48]
        uint64_t trim_seqnum;   // [40:48]
    };
    uint64_t client_data;       // [48:56]

    uint64_t prev_found_seqnum; // [56:64]

} __attribute__ (( packed, aligned(__FAAS_CACHE_LINE_SIZE) ));

static_assert(sizeof(SharedLogMessage) == 64, "Unexpected SharedLogMessage size");

class MessageHelper {
public:
    static bool IsLauncherHandshake(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::LAUNCHER_HANDSHAKE;
    }

    static bool IsFuncWorkerHandshake(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::FUNC_WORKER_HANDSHAKE;
    }

    static bool IsHandshakeResponse(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::HANDSHAKE_RESPONSE;
    }

    static bool IsCreateFuncWorker(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::CREATE_FUNC_WORKER;
    }

    static bool IsInvokeFunc(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::INVOKE_FUNC;
    }

    static bool IsDispatchFuncCall(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::DISPATCH_FUNC_CALL;
    }

    static bool IsFuncCallComplete(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::FUNC_CALL_COMPLETE;
    }

    static bool IsFuncCallFailed(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::FUNC_CALL_FAILED;
    }

    static bool IsSharedLogOp(const Message& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::SHARED_LOG_OP;
    }

    static void SetFuncCall(Message* message, const FuncCall& func_call) {
        message->func_id = func_call.func_id;
        message->method_id = func_call.method_id;
        message->client_id = func_call.client_id;
        message->call_id = func_call.call_id;
    }

    static FuncCall GetFuncCall(const Message& message) {
        DCHECK(IsInvokeFunc(message) || IsDispatchFuncCall(message)
                || IsFuncCallComplete(message) || IsFuncCallFailed(message)
                || IsSharedLogOp(message));
        FuncCall func_call;
        func_call.func_id = message.func_id;
        func_call.method_id = message.method_id;
        func_call.client_id = message.client_id;
        func_call.call_id = message.call_id;
        func_call.padding = 0;
        return func_call;
    }

    template<class T>
    static void SetInlineData(Message* message, std::span<const T> data) {
        size_t total_size = data.size() * sizeof(T);
        DCHECK(total_size <= MESSAGE_INLINE_DATA_SIZE);
        message->payload_size = gsl::narrow_cast<int32_t>(total_size);
        if (total_size > 0) {
            memcpy(message->inline_data, data.data(), total_size);
        }
    }

    template<class T>
    static void AppendInlineData(Message* message, std::span<const T> data) {
        size_t total_size = data.size() * sizeof(T);
        DCHECK_GE(message->payload_size, 0);
        if (total_size == 0) {
            return;
        }
        size_t tail = static_cast<size_t>(message->payload_size);
        DCHECK(tail + total_size <= MESSAGE_INLINE_DATA_SIZE);
        message->payload_size = gsl::narrow_cast<int32_t>(tail + total_size);
        memcpy(message->inline_data + tail, data.data(), total_size);
    }

    static void SetInlineData(Message* message, const std::string& data) {
        SetInlineData<char>(message, STRING_AS_SPAN(data));
    }

    static std::span<const char> GetInlineData(const Message& message) {
        if (IsInvokeFunc(message) || IsDispatchFuncCall(message)
              || IsFuncCallComplete(message) || IsLauncherHandshake(message)
              || IsSharedLogOp(message)) {
            if (message.payload_size > 0) {
                return std::span<const char>(
                    message.inline_data, gsl::narrow_cast<size_t>(message.payload_size));
            }
        }
        return EMPTY_CHAR_SPAN;
    }

    static SharedLogOpType GetSharedLogOpType(const Message& message) {
        return static_cast<SharedLogOpType>(message.log_op);
    }

    static SharedLogResultType GetSharedLogResultType(const Message& message) {
        return static_cast<SharedLogResultType>(message.log_result);
    }

    static int32_t ComputeMessageDelay(const Message& message) {
        if (message.send_timestamp > 0) {
            return gsl::narrow_cast<int32_t>(GetMonotonicMicroTimestamp() - message.send_timestamp);
        } else {
            return -1;
        }
    }

#define NEW_EMPTY_MESSAGE(MSG_VAR) \
    Message MSG_VAR; memset(&MSG_VAR, 0, sizeof(Message))

    static Message NewLauncherHandshake(uint16_t func_id) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::LAUNCHER_HANDSHAKE);
        message.func_id = func_id;
        return message;
    }

    static Message NewFuncWorkerHandshake(uint16_t func_id, uint16_t client_id) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_WORKER_HANDSHAKE);
        message.func_id = func_id;
        message.client_id = client_id;
        return message;
    }

    static Message NewHandshakeResponse(uint32_t payload_size) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::HANDSHAKE_RESPONSE);
        message.payload_size = gsl::narrow_cast<int32_t>(payload_size);
        return message;
    }

    static Message NewCreateFuncWorker(uint16_t client_id) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::CREATE_FUNC_WORKER);
        message.client_id = client_id;
        return message;
    }

    static Message NewInvokeFunc(const FuncCall& func_call, uint64_t parent_call_id,
                                 bool async = false) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::INVOKE_FUNC);
        SetFuncCall(&message, func_call);
        message.parent_call_id = parent_call_id;
        if (async) {
            message.flags |= kAsyncInvokeFuncFlag;
        }
        return message;
    }

    static Message NewDispatchFuncCall(const FuncCall& func_call) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::DISPATCH_FUNC_CALL);
        SetFuncCall(&message, func_call);
        return message;
    }

    static Message NewFuncCallComplete(const FuncCall& func_call, int32_t processing_time) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_COMPLETE);
        SetFuncCall(&message, func_call);
        message.processing_time = processing_time;
        return message;
    }

    static Message NewFuncCallFailed(const FuncCall& func_call) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_FAILED);
        SetFuncCall(&message, func_call);
        return message;
    }

    static Message NewSharedLogOpSucceeded(SharedLogResultType result,
                                           uint64_t log_seqnum = kInvalidLogSeqNum) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::SHARED_LOG_OP);
        message.log_op = static_cast<uint16_t>(SharedLogOpType::RESPONSE);
        message.log_result = static_cast<uint16_t>(result);
        message.log_seqnum = log_seqnum;
        return message;
    }

    static Message NewSharedLogOpFailed(SharedLogResultType result) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::SHARED_LOG_OP);
        message.log_op = static_cast<uint16_t>(SharedLogOpType::RESPONSE);
        message.log_result = static_cast<uint16_t>(result);
        return message;
    }

#undef NEW_EMPTY_MESSAGE

    static uint64_t GetAuxBufferId(const Message& message) {
        if ((message.flags & kUseAuxBufferFlag) == 0) {
            return kInvalidAuxBufId;
        }
        uint64_t id;
        memcpy(&id, message.inline_data, sizeof(uint64_t));
        return id;
    }

    static void FillAuxBufferId(Message* message, uint64_t buf_id) {
        DCHECK_EQ(message->payload_size, 0);
        message->flags |= kUseAuxBufferFlag;
        memcpy(message->inline_data, &buf_id, sizeof(uint64_t));
    }

private:
    DISALLOW_IMPLICIT_CONSTRUCTORS(MessageHelper);
};

class GatewayMessageHelper {
public:
    static bool IsDispatchFuncCall(const GatewayMessage& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::DISPATCH_FUNC_CALL;
    }

    static bool IsFuncCallComplete(const GatewayMessage& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::FUNC_CALL_COMPLETE;
    }

    static bool IsFuncCallFailed(const GatewayMessage& message) {
        return static_cast<MessageType>(message.message_type) == MessageType::FUNC_CALL_FAILED;
    }

    static void SetFuncCall(GatewayMessage* message, const FuncCall& func_call) {
        message->func_id = func_call.func_id;
        message->method_id = func_call.method_id;
        message->client_id = func_call.client_id;
        message->call_id = func_call.call_id;
    }

    static FuncCall GetFuncCall(const GatewayMessage& message) {
        DCHECK(IsDispatchFuncCall(message) || IsFuncCallComplete(message)
                  || IsFuncCallFailed(message));
        FuncCall func_call;
        func_call.func_id = message.func_id;
        func_call.method_id = message.method_id;
        func_call.client_id = message.client_id;
        func_call.call_id = message.call_id;
        func_call.padding = 0;
        return func_call;
    }

#define NEW_EMPTY_GATEWAY_MESSAGE(MSG_VAR) \
    GatewayMessage MSG_VAR; memset(&MSG_VAR, 0, sizeof(GatewayMessage))

    static GatewayMessage NewDispatchFuncCall(const FuncCall& func_call, uint32_t logspace = 0) {
        NEW_EMPTY_GATEWAY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::DISPATCH_FUNC_CALL);
        SetFuncCall(&message, func_call);
        message.logspace = logspace;
        return message;
    }

    static GatewayMessage NewFuncCallComplete(const FuncCall& func_call, int32_t processing_time) {
        NEW_EMPTY_GATEWAY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_COMPLETE);
        SetFuncCall(&message, func_call);
        message.processing_time = processing_time;
        return message;
    }

    static GatewayMessage NewFuncCallFailed(const FuncCall& func_call, int32_t status_code = 0) {
        NEW_EMPTY_GATEWAY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_FAILED);
        SetFuncCall(&message, func_call);
        message.status_code = status_code;
        return message;
    }

#undef NEW_EMPTY_GATEWAY_MESSAGE

private:
    DISALLOW_IMPLICIT_CONSTRUCTORS(GatewayMessageHelper);
};

class SharedLogMessageHelper {
public:
    static SharedLogOpType GetOpType(const SharedLogMessage& message) {
        return static_cast<SharedLogOpType>(message.op_type);
    }

    static SharedLogResultType GetResultType(const SharedLogMessage& message) {
        return static_cast<SharedLogResultType>(message.op_result);
    }

#define NEW_EMPTY_SHAREDLOG_MESSAGE(MSG_VAR) \
    SharedLogMessage MSG_VAR; memset(&MSG_VAR, 0, sizeof(SharedLogMessage))

    static SharedLogMessage NewReplicateMessage() {
        NEW_EMPTY_SHAREDLOG_MESSAGE(message);
        message.op_type = static_cast<uint16_t>(SharedLogOpType::REPLICATE);
        return message;
    }

    static SharedLogMessage NewSetAuxDataMessage(uint64_t seqnum) {
        NEW_EMPTY_SHAREDLOG_MESSAGE(message);
        message.op_type = static_cast<uint16_t>(SharedLogOpType::SET_AUXDATA);
        message.logspace_id = bits::HighHalf64(seqnum);
        message.seqnum_lowhalf = bits::LowHalf64(seqnum);
        return message;
    }

    static SharedLogMessage NewMetaLogsMessage(uint32_t logspace_id) {
        NEW_EMPTY_SHAREDLOG_MESSAGE(message);
        message.op_type = static_cast<uint16_t>(SharedLogOpType::METALOGS);
        message.logspace_id = logspace_id;
        return message;
    }

    static SharedLogMessage NewMetaLogProgressMessage(uint32_t logspace_id, uint32_t progress) {
        NEW_EMPTY_SHAREDLOG_MESSAGE(message);
        message.op_type = static_cast<uint16_t>(SharedLogOpType::META_PROG);
        message.logspace_id = logspace_id;
        message.metalog_position = progress;
        return message;
    }

    static SharedLogMessage NewShardProgressMessage(uint32_t logspace_id) {
        NEW_EMPTY_SHAREDLOG_MESSAGE(message);
        message.op_type = static_cast<uint16_t>(SharedLogOpType::SHARD_PROG);
        message.logspace_id = logspace_id;
        return message;
    }

    static SharedLogMessage NewIndexDataMessage(uint32_t logspace_id) {
        NEW_EMPTY_SHAREDLOG_MESSAGE(message);
        message.op_type = static_cast<uint16_t>(SharedLogOpType::INDEX_DATA);
        message.logspace_id = logspace_id;
        return message;
    }

    static SharedLogMessage NewReadMessage(SharedLogOpType op_type) {
        NEW_EMPTY_SHAREDLOG_MESSAGE(message);
        message.op_type = static_cast<uint16_t>(op_type);
        return message;
    }

    static SharedLogMessage NewReadAtMessage(uint32_t logspace_id, uint32_t seqnum_lowhalf) {
        NEW_EMPTY_SHAREDLOG_MESSAGE(message);
        message.op_type = static_cast<uint16_t>(SharedLogOpType::READ_AT);
        message.logspace_id = logspace_id;
        message.seqnum_lowhalf = seqnum_lowhalf;
        return message;
    }

    static SharedLogMessage NewResponse(SharedLogResultType result) {
        NEW_EMPTY_SHAREDLOG_MESSAGE(message);
        message.op_type = static_cast<uint16_t>(SharedLogOpType::RESPONSE);
        message.op_result = static_cast<uint16_t>(result);
        return message;
    }

    static SharedLogMessage NewReadOkResponse() {
        return NewResponse(SharedLogResultType::READ_OK);
    }

    static SharedLogMessage NewDataLostResponse() {
        return NewResponse(SharedLogResultType::DATA_LOST);
    }

#undef NEW_EMPTY_SHAREDLOG_MESSAGE

private:
    DISALLOW_IMPLICIT_CONSTRUCTORS(SharedLogMessageHelper);
};

}  // namespace protocol
}  // namespace faas
