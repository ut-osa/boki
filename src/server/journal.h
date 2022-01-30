#pragma once

#include "base/common.h"
#include "utils/appendable_buffer.h"
#include "utils/object_pool.h"
#include "utils/ref_count.h"

namespace faas {
namespace server {

class IOWorker;

class JournalFile : public RefCountedBase<JournalFile> {
public:
    JournalFile(IOWorker* owner, int file_id);
    ~JournalFile();

    IOWorker* owner() const { return owner_; }
    int file_id() const { return file_id_; }
    std::string_view file_path() const { return file_path_; }
    int fd() const { return fd_; }
    bool closed() const { return current_state() == kClosed; }

    size_t num_records() const { return num_records_; }
    size_t appended_bytes() const { return appended_bytes_; }
    size_t flushed_bytes() const { return flushed_bytes_; }

    bool ReachLimit() const;

    using AppendCallback = std::function<void(JournalFile* /* file */, size_t /* offset */)>;
    void AppendRecord(uint16_t type,
                      std::initializer_list<std::span<const char>> payload_vec,
                      AppendCallback cb);

    size_t ReadRecord(size_t offset, uint16_t* type, utils::AppendableBuffer* buffer);

    void Finalize();
    void Remove();

private:
    enum State { kEmpty, kActive, kFinalizing, kFinalized, kClosing, kClosed, kRemoved };

    std::atomic<State> state_;
    IOWorker* owner_;
    int file_id_;
    std::string file_path_;
    int fd_;

    size_t num_records_;
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

    void Create(int file_id);
    void CloseFd();
    void ScheduleFlush();
    void FlushRecords();
    void DataFlushed(size_t delta_bytes);

    // We use acquire-release memory model for state
    State current_state() const {
        return state_.load(std::memory_order_acquire);
    }
    void transit_state(State new_state) {
        state_.store(new_state, std::memory_order_release);
    }

    void RefBecomesZero() override;

    DISALLOW_COPY_AND_ASSIGN(JournalFile);
};

using JournalFileRef = JournalFile::Accessor;

}  // namespace server
}  // namespace faas
