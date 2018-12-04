/*
 *  Copyright (c) 2015-present, Facebook, Inc.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */
#include "BinarySerialized.h"

#include "mcrouter/lib/IOBufUtil.h"
#include "mcrouter/lib/McResUtil.h"
#include "mcrouter/lib/network/BinaryHeader.h"

using namespace facebook::memcache::binary_protocol;

namespace facebook {
namespace memcache {

size_t BinarySerializedMessage::getSize() const {
  return iovsTotalLen_;
}

void BinarySerializedMessage::write(folly::ByteRange range) {
  assert(iovsCount_ < kMaxIovs);
  auto bufLen = range.size();
  iovs_[iovsCount_].iov_base = const_cast<unsigned char*>(range.begin());
  iovs_[iovsCount_].iov_len = bufLen;
  ++iovsCount_;
  iovsTotalLen_ += bufLen;
}

void BinarySerializedMessage::write(folly::StringPiece s) {
  write(folly::ByteRange(s));
}

void BinarySerializedMessage::writeRequestHeader(uint8_t opCode, uint16_t keyLen, uint8_t valueLen, uint8_t extrasLen) {
  RequestHeader_t header = {
    .magic = 0x80,
    .opCode = opCode,
    .keyLen = keyLen,
    .extrasLen = extrasLen,
    .dataType = 0,
    .vBucketId = 0,
    .totalBodyLen = keyLen + valueLen + extrasLen,
    .opaque = 0,
    .cas = 0, // TODO
  };

  folly::ByteRange range(reinterpret_cast<const unsigned char*>(&header), sizeof(header));
  write(range);
}

void BinarySerializedMessage::writeResponseHeader(uint8_t opCode, uint16_t keyLen, uint8_t valueLen, uint8_t extrasLen) {
  ResponseHeader_t header = {
    .magic = 0x81,
    .opCode = opCode,
    .keyLen = keyLen,
    .extrasLen = extrasLen,
    .dataType = 0,
    .status = 0,
    .totalBodyLen = keyLen + valueLen + extrasLen,
    .opaque = 0,
    .cas = 0, // TODO
  };

  folly::ByteRange range(reinterpret_cast<const unsigned char*>(&header), sizeof(header));
  write(range);
}

void BinarySerializedMessage::prepareImpl(const McSetRequest& request) {
    LOG(INFO) << "serializing set request";
    auto value = coalesceAndGetRange(const_cast<folly::IOBuf&>(request.value()));
    writeRequestHeader(0x01, request.key().fullKey().size(), value.size(), 24);

    SetExtras_t extras = {};
    folly::ByteRange range(reinterpret_cast<const unsigned char*>(&extras), sizeof(extras));
    write(range);
    write(request.key().fullKey());
    write(value);
}

void BinarySerializedMessage::prepareImpl(McSetReply&& reply) {

}

void BinarySerializedMessage::clear() {
  iovsCount_ = 0;
  iobuf_.clear();
  auxString_.clear();
}

}
} // facebook::memcache
