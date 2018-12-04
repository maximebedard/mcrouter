/*
 *  Copyright (c) 2015-present, Facebook, Inc.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */
#pragma once

#include <folly/Optional.h>
#include <folly/Range.h>

#include "mcrouter/lib/McOperation.h"

namespace facebook {
namespace memcache {

/**
 * Class for serializing requests in binary protocol.
 */
class BinarySerializedMessage {
 public:
  BinarySerializedMessage() = default;

  BinarySerializedMessage(const BinarySerializedMessage&) = delete;
  BinarySerializedMessage& operator=(const BinarySerializedMessage&) = delete;
  BinarySerializedMessage(BinarySerializedMessage&&) = delete;
  BinarySerializedMessage& operator=(BinarySerializedMessage&&) = delete;

  void clear();

  /**
   * Prepare buffers for given Request.
   *
   * @param request
   * @param iovOut   will be set to the beginning of array of ivecs that reference serialized data.
   * @param niovOut  number of valid iovecs referenced by iovOut.
   * @return true    when the message was successfully prepared.
   */
  template <class Request>
  bool prepare(const Request& request, const struct iovec*& iovOut, size_t& niovOut);

  template <class Reply>
  bool prepare(Reply&& reply, size_t reqId, const struct iovec*& iovOut, size_t& niovOut);

  size_t getSize() const;

 private:
  // We need at most 5 iovecs (lease-set):
  //   command + key + printBuffer + value + "\r\n"
  static constexpr size_t kMaxIovs = 8;
  // The longest print buffer we need is for lease-set/cas operations.
  // It requires 2 uint64, 2 uint32 + 4 spaces + "\r\n" + '\0' = 67 chars.
  static constexpr size_t kMaxBufferLength = 80;

  struct iovec iovs_[kMaxIovs];
  size_t iovsCount_{0};
  size_t iovsTotalLen_{0};
  char printBuffer_[kMaxBufferLength];

  folly::Optional<folly::IOBuf> iobuf_;
  folly::Optional<std::string> auxString_;

  void write(folly::ByteRange);
  void write(folly::StringPiece);

  void writeRequestHeader(uint8_t opCode, uint16_t keyLen, uint8_t valueLen, uint8_t extrasLen);
  void writeResponseHeader(uint8_t opCode, uint16_t keyLen, uint8_t valueLen, uint8_t extrasLen);


  void prepareImpl(const McSetRequest& request);
  void prepareImpl(McSetReply&& reply);
  // Everything else is false.
  template <class Request>
  std::false_type prepareImpl(const Request& request);

  template <class Reply>
  std::false_type prepareImpl(Reply&& reply);

  struct PrepareImplWrapper;
};

}
} // facebook::memcache

#include "BinarySerialized-inl.h"
