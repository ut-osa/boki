#include "server/journal.h"

#include "common/flags.h"
#include "common/protocol.h"
#include "server/io_worker.h"
#include "server/io_uring.h"
#include "utils/fs.h"
#include "utils/hash.h"

#include <fcntl.h>

ABSL_FLAG(bool, journal_file_openflag_dsync, false, "");

namespace faas {
namespace server {

JournalFile::JournalFile(IOWorker* owner, int file_id)
    : state_(kEmpty),
      owner_(owner),
      fd_(-1),
      ref_count_(1),
      appended_bytes_(0),
      flushed_bytes_(0),
      flush_fn_scheduled_(false) {
    Create(file_id);
}

JournalFile::~JournalFile() {
    DCHECK(current_state() == kClosed || current_state() == kRemoved);
}

void JournalFile::AppendRecord(uint16_t type,
                               std::initializer_list<std::span<const char>> payload_vec,
                               AppendCallback cb) {
    DCHECK(owner_->WithinMyEventLoopThread());
    DCHECK(current_state() == kActive);
    size_t payload_size = 0;
    for (const auto& payload : payload_vec) {
        payload_size += payload.size();
    }
    protocol::JournalRecordHeader hdr = {
        .type         = type,
        .payload_size = gsl::narrow_cast<uint32_t>(payload_size),
        .timestamp    = GetRealtimeNanoTimestamp(),
        .checksum     = hash::xxHash64(payload_vec),
    };
    size_t record_size = sizeof(hdr) + payload_size;
    OngoingAppend* append_op = append_op_pool_.Get();
    append_op->offset = appended_bytes_;
    append_op->record_size = record_size;
    append_op->cb = std::move(cb);
    ongoing_appends_[append_op->offset] = append_op;
    write_buffer_.AppendData(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
    for (const auto& payload : payload_vec) {
        write_buffer_.AppendData(payload);
    }
    appended_bytes_ += record_size;
    ScheduleFlush();
}

namespace {
void ReadBytes(int fd, size_t offset, size_t size, char* buffer) {
    size_t pos = 0;
    while (pos < size) {
        ssize_t nread = pread(fd, buffer + pos, size - pos,
                              static_cast<off_t>(offset + pos));
        if (nread == 0) {
            LOG(FATAL) << "Reach the end of file!";
        }
        if (nread < 0) {
            if (errno == EAGAIN || errno == EINTR) {
                continue;
            }
            PLOG(FATAL) << "pread failed";
        }
        pos += static_cast<size_t>(nread);
    }
}
}  // namespace

size_t JournalFile::ReadRecord(size_t offset, uint16_t* type,
                               utils::AppendableBuffer* buffer) {
    State state = current_state();
    if (state == kClosing || state == kClosed || state == kRemoved) {
        LOG_F(FATAL, "Journal file {} has closed, cannot read from it", file_path_);
    }
    protocol::JournalRecordHeader hdr;
    ReadBytes(fd_, offset, sizeof(hdr), reinterpret_cast<char*>(&hdr));
    buffer->AppendUninitializedData(hdr.payload_size);
    char* buf_ptr = buffer->data() + (buffer->length() - hdr.payload_size);
    ReadBytes(fd_, offset + sizeof(hdr), hdr.payload_size, buf_ptr);
    std::span<const char> record_data(buf_ptr, hdr.payload_size);
    if (hdr.checksum != hash::xxHash64(record_data)) {
        LOG(FATAL) << "Checksum check failed for journal record";
    }
    *type = hdr.type;
    return hdr.payload_size;
}

void JournalFile::Finalize() {
    DCHECK(owner_->WithinMyEventLoopThread());
    DCHECK(current_state() == kActive);
    if (flushed_bytes_ == appended_bytes_) {
        transit_state(kFinalized);
        Unref();
    } else {
        transit_state(kFinalizing);
    }
}

void JournalFile::Remove() {
    DCHECK(current_state() == kClosed);
    if (!fs_utils::Remove(file_path_)) {
        LOG(FATAL) << "Failed to remove file " << file_path_;
    }
    LOG_F(INFO, "Journal file {} removed", file_path_);
    transit_state(kRemoved);
}

void JournalFile::RefBecomesZero() {
    owner_->ScheduleFunction(
        nullptr, absl::bind_front(&JournalFile::CloseFd, this));
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
    int flags = O_RDWR | O_APPEND | O_NONBLOCK;
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
    transit_state(kActive);
}

void JournalFile::CloseFd() {
    DCHECK(owner_->WithinMyEventLoopThread());
    DCHECK(current_state() == kFinalized);
    DCHECK_EQ(appended_bytes_, flushed_bytes_);
    transit_state(kClosing);
    URING_DCHECK_OK(owner_->io_uring()->Close(fd_, [this] () {
        LOG_F(INFO, "Journal file {} closed", file_path_);
        fd_ = -1;
        transit_state(kClosed);
    }));
}

void JournalFile::ScheduleFlush() {
    DCHECK(owner_->WithinMyEventLoopThread());
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
            } else if (flushed_bytes_ == appended_bytes_ && current_state() == kFinalizing) {
                transit_state(kFinalized);
                Unref();
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
