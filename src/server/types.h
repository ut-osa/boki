#pragma once

#include "base/common.h"

namespace faas {
namespace server {

// Forward declarations
class IOUring;
class IOWorker;
class ServerBase;
class JournalFile;
class Timer;

// ConnectionBase class
class ConnectionBase : public std::enable_shared_from_this<ConnectionBase> {
public:
    explicit ConnectionBase(int type = -1) : type_(type), id_(-1) {}
    virtual ~ConnectionBase() = default;

    int type() const { return type_; }
    int id() const { return id_; }

    template<class T>
    T* as_ptr() { return static_cast<T*>(this); }
    std::shared_ptr<ConnectionBase> ref_self() { return shared_from_this(); }

    virtual void Start(IOWorker* io_worker) = 0;
    virtual void ScheduleClose() = 0;

    // Only used for transferring connection from Server to IOWorker
    void set_id(int id) { id_ = id; }
    char* pipe_write_buf_for_transfer() { return pipe_write_buf_for_transfer_; }

protected:
    int type_;
    int id_;

    static IOUring* current_io_uring();

private:
    char pipe_write_buf_for_transfer_[__FAAS_PTR_SIZE];

    DISALLOW_COPY_AND_ASSIGN(ConnectionBase);
};

// Callback types
using TimerCallback = std::function<void()>;
using JournalAppendCallback = std::function<void(JournalFile* /* file */,
                                                 size_t /* offset */)>;

// Others
struct JournalStat {
    int    num_created_files;
    int    num_closed_files;
    size_t total_bytes;
    size_t total_records;
};

}  // namespace server
}  // namespace faas
