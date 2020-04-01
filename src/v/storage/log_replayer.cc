#include "storage/log_replayer.h"

#include "hashing/crc32c.h"
#include "likely.h"
#include "model/record.h"
#include "model/record_utils.h"
#include "storage/logger.h"
#include "storage/parser.h"
#include "utils/vint.h"
#include "vlog.h"

#include <limits>
#include <type_traits>

namespace storage {
static inline void crc_extend_iobuf(crc32& crc, const iobuf& buf) {
    auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
    (void)in.consume(buf.size_bytes(), [&crc](const char* src, size_t sz) {
        // NOLINTNEXTLINE
        crc.extend(reinterpret_cast<const uint8_t*>(src), sz);
        return ss::stop_iteration::no;
    });
}
class checksumming_consumer final : public batch_consumer {
public:
    static constexpr size_t max_segment_size = static_cast<size_t>(
      std::numeric_limits<uint32_t>::max());
    checksumming_consumer(segment* s, log_replayer::checkpoint& c)
      : _seg(s)
      , _cfg(c) {
        // we'll reconstruct the state manually
        _seg->index().reset();
    }
    checksumming_consumer(const checksumming_consumer&) = delete;
    checksumming_consumer& operator=(const checksumming_consumer&) = delete;
    checksumming_consumer(checksumming_consumer&&) noexcept = delete;
    checksumming_consumer& operator=(checksumming_consumer&&) noexcept = delete;
    ~checksumming_consumer() noexcept override = default;

    consume_result consume_batch_start(
      model::record_batch_header header,
      size_t physical_base_offset,
      size_t size_on_disk) override {
        _header = header;
        _current_batch_crc = header.crc;
        _file_pos_to_end_of_batch = size_on_disk + physical_base_offset;
        _last_offset = header.last_offset();
        _crc = crc32();
        model::crc_record_batch_header(_crc, header);
        return skip_batch::no;
    }

    consume_result consume_record(model::record r) override {
        model::crc_record(_crc, r);
        return skip_batch::no;
    }
    void consume_compressed_records(iobuf&& records) override {
        crc_extend_iobuf(_crc, records);
    }

    stop_parser consume_batch_end() override {
        if (is_valid_batch_crc()) {
            _cfg.last_offset = _last_offset;
            _cfg.truncate_file_pos = _file_pos_to_end_of_batch;
            const auto physical_base_offset = _file_pos_to_end_of_batch
                                              - _header.size_bytes;
            _seg->index().maybe_track(_header, physical_base_offset);
            _header = {};
            return stop_parser::no;
        }
        return stop_parser::yes;
    }

    bool is_valid_batch_crc() const {
        return _current_batch_crc == _crc.value();
    }

private:
    model::record_batch_header _header;
    segment* _seg;
    log_replayer::checkpoint& _cfg;
    int32_t _current_batch_crc{0};
    crc32 _crc;
    model::offset _last_offset;
    size_t _file_pos_to_end_of_batch{0};
};

// Called in the context of a ss::thread
log_replayer::checkpoint
log_replayer::recover_in_thread(const ss::io_priority_class& prio) {
    vlog(stlog.debug, "Recovering segment {}", *_seg);
    // explicitly not using the index to recover the full file
    auto data_stream = _seg->reader().data_stream(0, prio);
    auto consumer = std::make_unique<checksumming_consumer>(_seg, _ckpt);
    auto parser = continuous_batch_parser(
      std::move(consumer), std::move(data_stream));
    try {
        parser.consume().get();
        parser.close().get();
    } catch (...) {
        vlog(
          stlog.warn,
          "{} partial recovery to {}, with: {}",
          _seg->reader().filename(),
          _ckpt,
          std::current_exception());
    }
    return _ckpt;
}

std::ostream& operator<<(std::ostream& o, const log_replayer::checkpoint& c) {
    o << "{ last_offset: ";
    if (c.last_offset) {
        o << *c.last_offset;
    } else {
        o << "null";
    }
    o << ", truncate_file_pos:";
    if (c.truncate_file_pos) {
        o << *c.truncate_file_pos;
    } else {
        o << "null";
    }
    return o << "}";
}
} // namespace storage
