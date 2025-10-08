#pragma once

#include <vector>
#include <cstring>
#include <cstdint>
#include <memory>
#include "utils/status.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {
namespace db {

/**
 * Binary Protocol for VectorDB
 *
 * Format:
 * - Header (16 bytes):
 *   - Magic number: 4 bytes (0x56454342 = "VECB")
 *   - Version: 2 bytes
 *   - Message type: 2 bytes
 *   - Payload size: 4 bytes
 *   - Checksum: 4 bytes
 *
 * - Payload:
 *   - Batch insert format:
 *     - Batch size: 4 bytes
 *     - Dimension: 4 bytes
 *     - For each record:
 *       - ID: 8 bytes (int64)
 *       - Vector: dimension * 4 bytes (float32)
 */

enum class MessageType : uint16_t {
    BATCH_INSERT = 0x0001,
    BATCH_UPDATE = 0x0002,
    BATCH_DELETE = 0x0003,
    QUERY = 0x0004,
    RESPONSE = 0x0005
};

struct BinaryHeader {
    uint32_t magic;
    uint16_t version;
    uint16_t message_type;
    uint32_t payload_size;
    uint32_t checksum;

    static constexpr uint32_t MAGIC_NUMBER = 0x56454342; // "VECB"
    static constexpr uint16_t CURRENT_VERSION = 1;
    static constexpr size_t HEADER_SIZE = 16;

    BinaryHeader()
        : magic(MAGIC_NUMBER)
        , version(CURRENT_VERSION)
        , message_type(0)
        , payload_size(0)
        , checksum(0) {}

    bool IsValid() const {
        return magic == MAGIC_NUMBER && version == CURRENT_VERSION;
    }
};

class BinaryProtocol {
private:
    mutable Logger logger_;

    uint32_t CalculateChecksum(const uint8_t* data, size_t size) {
        // Simple CRC32 or Adler32 implementation
        uint32_t checksum = 1;
        uint32_t s1 = checksum & 0xffff;
        uint32_t s2 = (checksum >> 16) & 0xffff;

        for (size_t i = 0; i < size; i++) {
            s1 = (s1 + data[i]) % 65521;
            s2 = (s2 + s1) % 65521;
        }

        return (s2 << 16) | s1;
    }

public:
    BinaryProtocol() {}

    /**
     * Encode batch insert data to binary format
     */
    std::vector<uint8_t> EncodeBatchInsert(
        const std::vector<int64_t>& ids,
        const std::vector<float>& vectors,
        size_t dimension) {

        size_t batch_size = ids.size();
        size_t payload_size = 8 + batch_size * (8 + dimension * 4);
        size_t total_size = BinaryHeader::HEADER_SIZE + payload_size;

        std::vector<uint8_t> buffer(total_size);
        uint8_t* ptr = buffer.data();

        // Write header
        BinaryHeader header;
        header.message_type = static_cast<uint16_t>(MessageType::BATCH_INSERT);
        header.payload_size = payload_size;

        memcpy(ptr, &header.magic, 4); ptr += 4;
        memcpy(ptr, &header.version, 2); ptr += 2;
        memcpy(ptr, &header.message_type, 2); ptr += 2;
        memcpy(ptr, &header.payload_size, 4); ptr += 4;

        // Skip checksum for now
        uint8_t* checksum_ptr = ptr;
        ptr += 4;

        // Write payload
        uint32_t batch_size_32 = batch_size;
        uint32_t dimension_32 = dimension;
        memcpy(ptr, &batch_size_32, 4); ptr += 4;
        memcpy(ptr, &dimension_32, 4); ptr += 4;

        // Write records
        for (size_t i = 0; i < batch_size; i++) {
            memcpy(ptr, &ids[i], 8); ptr += 8;
            memcpy(ptr, &vectors[i * dimension], dimension * 4);
            ptr += dimension * 4;
        }

        // Calculate and write checksum
        uint32_t checksum = CalculateChecksum(
            buffer.data() + BinaryHeader::HEADER_SIZE,
            payload_size
        );
        memcpy(checksum_ptr, &checksum, 4);

        logger_.Debug("Encoded batch insert: " + std::to_string(batch_size) +
                     " vectors, " + std::to_string(total_size) + " bytes");

        return buffer;
    }

    /**
     * Decode binary data to batch insert format
     */
    Status DecodeBatchInsert(
        const uint8_t* data,
        size_t data_size,
        std::vector<int64_t>& ids,
        std::vector<float>& vectors,
        size_t& dimension) {

        if (data_size < BinaryHeader::HEADER_SIZE) {
            return Status(StatusCode::INVALID_ARGUMENT,
                         "Data too small for header");
        }

        // Read header
        const uint8_t* ptr = data;
        BinaryHeader header;
        memcpy(&header.magic, ptr, 4); ptr += 4;
        memcpy(&header.version, ptr, 2); ptr += 2;
        memcpy(&header.message_type, ptr, 2); ptr += 2;
        memcpy(&header.payload_size, ptr, 4); ptr += 4;
        memcpy(&header.checksum, ptr, 4); ptr += 4;

        if (!header.IsValid()) {
            return Status(StatusCode::INVALID_ARGUMENT,
                         "Invalid header magic or version");
        }

        if (header.message_type != static_cast<uint16_t>(MessageType::BATCH_INSERT)) {
            return Status(StatusCode::INVALID_ARGUMENT,
                         "Not a batch insert message");
        }

        if (data_size < BinaryHeader::HEADER_SIZE + header.payload_size) {
            return Status(StatusCode::INVALID_ARGUMENT,
                         "Data size mismatch");
        }

        // Verify checksum
        uint32_t calculated_checksum = CalculateChecksum(ptr, header.payload_size);
        if (calculated_checksum != header.checksum) {
            return Status(StatusCode::INVALID_ARGUMENT,
                         "Checksum mismatch");
        }

        // Read payload
        uint32_t batch_size;
        uint32_t dimension_32;
        memcpy(&batch_size, ptr, 4); ptr += 4;
        memcpy(&dimension_32, ptr, 4); ptr += 4;
        dimension = dimension_32;

        // Allocate space
        ids.resize(batch_size);
        vectors.resize(batch_size * dimension);

        // Read records
        for (size_t i = 0; i < batch_size; i++) {
            memcpy(&ids[i], ptr, 8); ptr += 8;
            memcpy(&vectors[i * dimension], ptr, dimension * 4);
            ptr += dimension * 4;
        }

        logger_.Debug("Decoded batch insert: " + std::to_string(batch_size) +
                     " vectors, dimension " + std::to_string(dimension));

        return Status::OK();
    }

    /**
     * Calculate size reduction compared to JSON
     */
    double CalculateSizeReduction(size_t batch_size, size_t dimension) {
        // Binary size
        size_t binary_size = BinaryHeader::HEADER_SIZE + 8 +
                           batch_size * (8 + dimension * 4);

        // Estimated JSON size (rough approximation)
        // JSON: {"table":"vectors","records":[{"id":123,"vec":[0.1,0.2,...]},...]}
        // Overhead: ~50 bytes per record + 15 bytes per float
        size_t json_size = 50 + batch_size * (50 + dimension * 15);

        double reduction = 1.0 - (double)binary_size / json_size;

        logger_.Info("Size reduction: Binary " + std::to_string(binary_size) +
                    " bytes vs JSON ~" + std::to_string(json_size) +
                    " bytes (" + std::to_string(reduction * 100) + "% reduction)");

        return reduction;
    }
};

/**
 * Binary protocol handler for fast batch operations
 */
class BinaryBatchProcessor {
private:
    std::unique_ptr<BinaryProtocol> protocol_;
    mutable Logger logger_;

public:
    BinaryBatchProcessor()
        : protocol_(std::make_unique<BinaryProtocol>()) {}

    /**
     * Process binary batch insert with optimizations
     */
    Status ProcessBinaryInsert(
        const uint8_t* data,
        size_t data_size,
        std::function<Status(const std::vector<int64_t>&,
                           const std::vector<float>&,
                           size_t)> insert_callback) {

        std::vector<int64_t> ids;
        std::vector<float> vectors;
        size_t dimension;

        // Decode binary data
        auto status = protocol_->DecodeBatchInsert(
            data, data_size, ids, vectors, dimension
        );

        if (!status.ok()) {
            return status;
        }

        // Process with callback
        return insert_callback(ids, vectors, dimension);
    }

    // Removed ConvertJsonToBinary method to fix compilation
    // This would require proper Json API integration
};

} // namespace db
} // namespace engine
} // namespace vectordb