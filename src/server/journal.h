#pragma once

#include "base/common.h"
#include "utils/appendable_buffer.h"
#include "utils/object_pool.h"

namespace faas {
namespace server {

class IOWorker;

class JournalFile {
public:
    JournalFile(IOWorker* owner, int file_id);
    ~JournalFile();

    IOWorker* owner() const { return owner_; }
    std::string_view file_path() const { return file_path_; }
    int fd() const { return fd_; }
    bool closed() const { return state_ == kClosed; }

    size_t appended_bytes() const { return appended_bytes_; }
    size_t flushed_bytes() const { return flushed_bytes_; }

    using AppendCallback = std::function<void(JournalFile* /* file */, size_t /* offset */)>;
    void AppendRecord(uint16_t type, std::span<const char> payload, AppendCallback cb);

    void Close(std::function<void()> cb);
    void Remove();

private:
    enum State { kEmpty, kCreated, kClosing, kClosed, kRemoved };

    State state_;
    IOWorker* owner_;
    std::string file_path_;
    int fd_;

    size_t appended_bytes_;
    size_t flushed_bytes_;

    struct OngoingAppend {
        size_t          offset;
        size_t          record_size;
        AppendCallback  cb;
    };
    utils::SimpleObjectPool<OngoingAppend> append_op_pool_;
    absl::flat_hash_map</* offset */ size_t, OngoingAppend*> ongoing_appends_;

    utils::AppendableBuffer write_buffer_;
    utils::AppendableBuffer flush_buffer_;
    bool flush_fn_scheduled_;

    std::function<void()> close_cb_;

    void Create(int file_id);
    void CloseFd();
    void ScheduleFlush();
    void FlushRecords();
    void DataFlushed(size_t delta_bytes);

    DISALLOW_COPY_AND_ASSIGN(JournalFile);
};

}  // namespace server
}  // namespace faas
