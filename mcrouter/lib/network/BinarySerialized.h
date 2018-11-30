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
#include "mcrouter/lib/network/gen/Memcache.h"
#include "mcrouter/lib/network/gen/MemcacheRoutingGroups.h"

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

  template <class Request>
  bool prepare(
          const Request& request,
          folly::Optional<folly::IOBuf>& key,
          const struct iovec*& iovOut,
          size_t& niovOut);

  /**
   * Returns the size of the request.
   */
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

  void addString(folly::ByteRange range);
  void addString(folly::StringPiece str);

  template <class Arg1, class Arg2>
  void addStrings(Arg1&& arg1, Arg2&& arg2);
  template <class Arg, class... Args>
  void addStrings(Arg&& arg, Args&&... args);

  template <class Request>
  void keyValueRequestCommon(folly::StringPiece prefix, const Request& request);

  // Get-like ops.
  void prepareImpl(const McGetRequest& request);
  void prepareImpl(const McGetsRequest& request);
  void prepareImpl(const McMetagetRequest& request);
  void prepareImpl(const McLeaseGetRequest& request);
  // Update-like ops.
  void prepareImpl(const McSetRequest& request);
  void prepareImpl(const McAddRequest& request);
  void prepareImpl(const McReplaceRequest& request);
  void prepareImpl(const McAppendRequest& request);
  void prepareImpl(const McPrependRequest& request);
  void prepareImpl(const McCasRequest& request);
  void prepareImpl(const McLeaseSetRequest& request);
  // Arithmetic ops.
  void prepareImpl(const McIncrRequest& request);
  void prepareImpl(const McDecrRequest& request);
  // Delete op.
  void prepareImpl(const McDeleteRequest& request);
  // Touch op.
  void prepareImpl(const McTouchRequest& request);
  // Version op.
  void prepareImpl(const McVersionRequest& request);
  // FlushAll op.
  void prepareImpl(const McFlushAllRequest& request);

  // Everything else is false.
  template <class Request>
  std::false_type prepareImpl(const Request& request);

  struct PrepareImplWrapper;
};
}
} // facebook::memcache

#include "BinarySerialized-inl.h"
