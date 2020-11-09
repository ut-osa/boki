#pragma once

#include "base/common.h"
#include "common/uv.h"

namespace faas {
namespace server {

class IOWorker;

class ConnectionBase : public uv::Base, public std::enable_shared_from_this<ConnectionBase> {
public:
    explicit ConnectionBase(int type = -1) : type_(type), id_(-1), io_worker_(nullptr) {}
    virtual ~ConnectionBase() {}

    int type() const { return type_; }
    int id() const { return id_; }
    IOWorker* io_worker() { return io_worker_; }

    template<class T>
    T* as_ptr() { return static_cast<T*>(this); }
    std::shared_ptr<ConnectionBase> ref_self() { return shared_from_this(); }

    virtual uv_stream_t* InitUVHandle(uv_loop_t* uv_loop) = 0;
    virtual void Start() = 0;
    virtual void ScheduleClose() = 0;

    // Only used for transferring connection from Server to IOWorker
    void set_id(int id) { id_ = id; }
    void set_io_worker(IOWorker* io_worker) { io_worker_ = io_worker; }
    uv_write_t* uv_write_req_for_transfer() { return &uv_write_req_for_transfer_; }
    uv_write_t* uv_write_req_for_back_transfer() { return &uv_write_req_for_back_transfer_; }
    char* pipe_write_buf_for_transfer() { return pipe_write_buf_for_transfer_; }

protected:
    int type_;
    int id_;
    IOWorker* io_worker_;

private:
    uv_write_t uv_write_req_for_transfer_;
    uv_write_t uv_write_req_for_back_transfer_;
    char pipe_write_buf_for_transfer_[__FAAS_PTR_SIZE];

    DISALLOW_COPY_AND_ASSIGN(ConnectionBase);
};

}  // namespace server
}  // namespace faas
