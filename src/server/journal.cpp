#include "server/journal.h"

#include "common/flags.h"
#include "common/protocol.h"
#include "server/io_worker.h"
#include "server/io_uring.h"
#include "utils/fs.h"

#include <fcntl.h>

ABSL_FLAG(bool, journal_file_openflag_dsync, false, "");

namespace faas {
namespace server {

JournalFile::JournalFile(IOWorker* owner, int file_id)
    : state_(kEmpty),
      owner_(owner),
      fd_(-1),
      appended_bytes_(0),
      flushed_bytes_(0),
      flush_fn_scheduled_(false) {
    Create(file_id);
}

JournalFile::~JournalFile() {
    DCHECK(state_ == kClosed || state_ == kRemoved);
}

void JournalFile::AppendRecord(uint16_t type, std::span<const char> payload,
                               AppendCallback cb) {
    DCHECK(owner_->WithinMyEventLoopThread());
    DCHECK(state_ == kCreated);
    protocol::JournalRecordHeader hdr = {
        .type         = type,
        .payload_size = gsl::narrow_cast<uint16_t>(payload.size()),
        .timestamp    = GetRealtimeNanoTimestamp(),
    };
    size_t record_size = sizeof(hdr) + payload.size();
    OngoingAppend* append_op = append_op_pool_.Get();
    append_op->offset = appended_bytes_;
    append_op->record_size = record_size;
    append_op->cb = std::move(cb);
    ongoing_appends_[append_op->offset] = append_op;
    write_buffer_.AppendData(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
    write_buffer_.AppendData(payload);
    appended_bytes_ += record_size;
    ScheduleFlush();
}

void JournalFile::Close(std::function<void()> cb) {
    DCHECK(owner_->WithinMyEventLoopThread());
    if (state_ == kClosing) {
        LOG_F(WARNING, "Journal file {} is closing", file_path_);
        return;
    }
    if (state_ == kClosed) {
        LOG_F(WARNING, "Journal file {} already closed", file_path_);
        return;
    }
    close_cb_ = std::move(cb);
    state_ = kClosing;
    if (flushed_bytes_ == appended_bytes_) {
        CloseFd();
    }
}

void JournalFile::Remove() {
    if (state_ == kRemoved) {
        LOG_F(WARNING, "Journal file {} already removed", file_path_);
        return;
    }
    DCHECK(state_ == kClosed);
    if (!fs_utils::Remove(file_path_)) {
        LOG(FATAL) << "Failed to remove file " << file_path_;
    }
    LOG_F(INFO, "Journal file {} removed", file_path_);
    state_ = kRemoved;
}

void JournalFile::Create(int file_id) {
    std::string file_path = fs_utils::JoinPath(
        absl::GetFlag(FLAGS_journal_save_path),
        fmt::format("{}.{}", owner_->worker_name(), file_id));
    if (auto fd = fs_utils::Create(file_path); fd) {
        PCHECK(close(fd.value()) == 0) << "Failed to close file";
    } else {
        LOG(FATAL) << "Failed to create file " << file_path;
    }
    int flags = O_WRONLY | O_APPEND | O_NONBLOCK;
    if (absl::GetFlag(FLAGS_journal_file_openflag_dsync)) {
        flags |= O_DSYNC;
    }
    auto fd = fs_utils::Open(file_path, flags);
    if (!fd) {
        LOG(FATAL) << "Failed to open file " << file_path;
    }
    LOG_F(INFO, "Create journal file: {} (fd {})", file_path, fd.value());
    file_path_ = std::move(file_path);
    fd_ = fd.value();
    URING_DCHECK_OK(owner_->io_uring()->RegisterFd(fd_));
    state_ = kCreated;
}

void JournalFile::CloseFd() {
    DCHECK_EQ(appended_bytes_, flushed_bytes_);
    DCHECK(state_ == kClosing);
    URING_DCHECK_OK(owner_->io_uring()->Close(fd_, [this] () {
        LOG_F(INFO, "Journal file {} closed", file_path_);
        fd_ = -1;
        state_ = kClosed;
        close_cb_();
    }));
}

void JournalFile::ScheduleFlush() {
    DCHECK(owner_->WithinMyEventLoopThread());
    DCHECK(state_ == kCreated || state_ == kClosing);
    DCHECK(!write_buffer_.empty());
    if (!flush_fn_scheduled_) {
        owner_->ScheduleIdleFunction(
            nullptr, absl::bind_front(&JournalFile::FlushRecords, this));
        flush_fn_scheduled_ = true;
    }
}

void JournalFile::FlushRecords() {
    DCHECK(owner_->WithinMyEventLoopThread());
    DCHECK(flush_fn_scheduled_);
    flush_fn_scheduled_ = false;
    if (!flush_buffer_.empty()) {
        return;
    }
    flush_buffer_.Swap(write_buffer_);
    URING_DCHECK_OK(owner_->io_uring()->Write(
        fd_, flush_buffer_.to_span(),
        [this] (int status, size_t nwrite) {
            if (status != 0) {
                PLOG(FATAL) << "Failed to append journal";
            } else if (nwrite < flush_buffer_.length()) {
                PLOG_F(FATAL, "Partial write occurs: nwrite={}, expect={}",
                       nwrite, flush_buffer_.length());
            }
            DataFlushed(flush_buffer_.length());
            flush_buffer_.Reset();
            if (!write_buffer_.empty()) {
                ScheduleFlush();
            } else if (state_ == kClosing && flushed_bytes_ == appended_bytes_) {
                CloseFd();
            }
        }
    ));
}

void JournalFile::DataFlushed(size_t delta_bytes) {
    DCHECK(owner_->WithinMyEventLoopThread());
    size_t offset = flushed_bytes_;
    while (delta_bytes > 0) {
        DCHECK(ongoing_appends_.contains(offset));
        OngoingAppend* append_op = ongoing_appends_.at(offset);
        ongoing_appends_.erase(offset);
        DCHECK_EQ(append_op->offset, offset);
        append_op->cb(this, offset);
        offset += append_op->record_size;
        delta_bytes -= append_op->record_size;
        append_op_pool_.Return(append_op);
    }
    flushed_bytes_ = offset;
    DCHECK_LE(flushed_bytes_, appended_bytes_);
}

}  // namespace server
}  // namespace faas
