#include "server/journal.h"

#include "common/flags.h"
#include "server/io_worker.h"
#include "server/io_uring.h"
#include "utils/fs.h"
#include "utils/hash.h"

#include <fcntl.h>

ABSL_FLAG(bool, journal_file_openflag_dsync, false, "");
ABSL_FLAG(size_t, journal_file_max_records_per_flush, 4, "");
ABSL_FLAG(size_t, journal_file_cutoff_bytes_per_flush, 4096, "");

namespace faas {
namespace server {

using protocol::JournalRecordHeader;

JournalFile::JournalFile(IOWorker* owner, int file_id)
    : state_(kEmpty),
      owner_(owner),
      file_id_(file_id),
      fd_(-1),
      checksum_enabled_(!absl::GetFlag(FLAGS_journal_disable_checksum)),
      num_records_(0),
      appended_bytes_(0),
      flushed_bytes_(0),
      pending_head_(nullptr),
      scheduled_head_(nullptr),
      pending_tail_(nullptr),
      flush_fn_scheduled_(false) {
    append_op_pool_.SetObjectInitFn([] (OngoingAppend* op) {
        op->offset = std::numeric_limits<size_t>::max();
        memset(&op->header, 0, sizeof(JournalRecordHeader));
        op->data.Reset();
        op->cb = nullptr;
        op->next = nullptr;
    });
    Create(file_id);
}

JournalFile::~JournalFile() {
    State state = current_state();
    DCHECK(state == kFinalized || state == kClosed || state == kRemoved);
}

bool JournalFile::ReachLimit() const {
    size_t record_cap = absl::GetFlag(FLAGS_journal_file_max_records);
    if (record_cap > 0 && num_records_ >= record_cap) {
        return true;
    }
    size_t size_cap = absl::GetFlag(FLAGS_journal_file_max_size_mb) * 1024 * 1024;
    if (appended_bytes_ >= size_cap) {
        return true;
    }
    return false;
}

size_t JournalFile::AppendRecord(uint16_t type,
                                 std::initializer_list<std::span<const char>> payload_vec,
                                 AppendCallback cb) {
    DCHECK(owner_->WithinMyEventLoopThread());
    DCHECK(current_state() == kActive);
    size_t payload_size = 0;
    for (const auto& payload : payload_vec) {
        payload_size += payload.size();
    }
    if (payload_size > kMaxRecordPayloadSize) {
        LOG_F(FATAL, "Payload size ({}) too large for a single journal record!", payload_size);
    }
    size_t record_size = sizeof(JournalRecordHeader) + payload_size;
    JournalRecordHeader hdr = {
        .type         = type,
        .payload_size = gsl::narrow_cast<uint32_t>(payload_size),
        .record_size  = gsl::narrow_cast<uint32_t>(record_size),
        .timestamp    = GetRealtimeNanoTimestamp(),
        .checksum     = checksum_enabled_ ? hash::xxHash64(payload_vec) : uint64_t{0},
    };
    OngoingAppend* append_op = append_op_pool_.Get();
    append_op->offset = appended_bytes_;
    append_op->header = hdr;
    append_op->cb = std::move(cb);
    for (const auto& payload : payload_vec) {
        append_op->data.AppendData(payload);
    }
    if (pending_tail_ != nullptr) {
        pending_tail_->next = append_op;
    }
    pending_tail_ = append_op;
    if (pending_head_ == nullptr) {
        pending_head_ = append_op;
    }
    if (scheduled_head_ == nullptr) {
        scheduled_head_ = append_op;
    }
    VLOG_F(1, "Record with offset {} added to pending list", append_op->offset);
    num_records_++;
    appended_bytes_ += record_size;
    ScheduleFlush();
    return record_size;
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
    JournalRecordHeader hdr;
    ReadBytes(fd_, offset, sizeof(hdr), reinterpret_cast<char*>(&hdr));
    buffer->AppendUninitializedData(hdr.payload_size);
    char* buf_ptr = buffer->data() + (buffer->length() - hdr.payload_size);
    ReadBytes(fd_, offset + sizeof(hdr), hdr.payload_size, buf_ptr);
    std::span<const char> record_data(buf_ptr, hdr.payload_size);
    if (checksum_enabled_ && hdr.checksum != hash::xxHash64(record_data)) {
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
    owner_->ScheduleFunction(nullptr, [this] () {
        if (!fs_utils::Remove(file_path_)) {
            LOG(FATAL) << "Failed to remove file " << file_path_;
        }
        LOG_F(INFO, "Journal file {} removed", file_path_);
        transit_state(kRemoved);
        owner_->OnJournalFileRemoved(this);
    });
}

void JournalFile::RefBecomesZero() {
    owner_->ScheduleFunction(
        nullptr, absl::bind_front(&JournalFile::CloseFd, this));
}

void JournalFile::Create(int file_id) {
    std::string file_path = fs_utils::JoinPath(
        absl::GetFlag(FLAGS_journal_save_path), fmt::format("{}", file_id));
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
        owner_->OnJournalFileClosed(this);
    }));
}

void JournalFile::ScheduleFlush() {
    DCHECK(owner_->WithinMyEventLoopThread());
    DCHECK(scheduled_head_ != nullptr);
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
    FillFlushBuffer();
    DCHECK(!flush_buffer_.empty());
    owner_->RecordFlushBufferSize(flush_buffer_.length());
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
            if (scheduled_head_ != nullptr) {
                ScheduleFlush();
            } else if (flushed_bytes_ == appended_bytes_ && current_state() == kFinalizing) {
                transit_state(kFinalized);
                Unref();
            }
        }
    ));
}

void JournalFile::FillFlushBuffer() {
    DCHECK(owner_->WithinMyEventLoopThread());
    DCHECK(flush_buffer_.empty());
    if (scheduled_head_ == nullptr) {
        return;
    }
    const size_t kMaxRecords = absl::GetFlag(FLAGS_journal_file_max_records_per_flush);
    const size_t kCutoffBytes = absl::GetFlag(FLAGS_journal_file_cutoff_bytes_per_flush);
    size_t num_records = 0;
    while (num_records < kMaxRecords && scheduled_head_ != nullptr) {
        OngoingAppend* append_op = scheduled_head_;
        size_t new_size = flush_buffer_.length() + append_op->header.record_size;
        if (num_records > 0 && new_size > kCutoffBytes) {
            break;
        }
        flush_buffer_.AppendData(VAR_AS_CHAR_SPAN(append_op->header, JournalRecordHeader));
        flush_buffer_.AppendData(append_op->data.to_span());
        DCHECK_EQ(flush_buffer_.length(), new_size);
        VLOG_F(1, "Will flush record with offset {}", append_op->offset);
        num_records++;
        scheduled_head_ = append_op->next;
    }
    VLOG_F(1, "Fill flush buffer with {} records, total bytes {}",
           num_records, flush_buffer_.length());
}

void JournalFile::DataFlushed(size_t delta_bytes) {
    DCHECK(owner_->WithinMyEventLoopThread());
    size_t offset = flushed_bytes_;
    while (delta_bytes > 0) {
        OngoingAppend* append_op = DCHECK_NOTNULL(pending_head_);
        DCHECK_EQ(append_op->offset, offset);
        append_op->cb(this, offset);
        VLOG_F(1, "Record with offset {} flushed", append_op->offset);
        owner_->OnJournalRecordAppended(append_op->header);
        size_t record_size = append_op->header.record_size;
        offset += record_size;
        delta_bytes -= record_size;
        pending_head_ = append_op->next;
        append_op_pool_.Return(append_op);
    }
    flushed_bytes_ = offset;
    if (pending_head_ == nullptr) {
        pending_tail_ = nullptr;
    }
    DCHECK_LE(flushed_bytes_, appended_bytes_);
}

}  // namespace server
}  // namespace faas
