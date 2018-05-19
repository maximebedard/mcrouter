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

class BinarySerializedReply {
 public:
  BinarySerializedReply() = default;

  BinarySerializedReply(const BinarySerializedReply&) = delete;
  BinarySerializedReply& operator=(const BinarySerializedReply&) = delete;
  BinarySerializedReply(BinarySerializedReply&&) noexcept = delete;
  BinarySerializedReply& operator=(BinarySerializedReply&&) = delete;

  ~BinarySerializedReply() = default;

  void clear();

  template <class Reply>
  bool prepare(
      Reply&& reply,
      folly::Optional<folly::IOBuf>& key,
      const struct iovec*& iovOut,
      size_t& niovOut,
      carbon::GetLikeT<RequestFromReplyType<Reply, RequestReplyPairs>> = 0) {
    if (key.hasValue()) {
      key->coalesce();
    }
    prepareImpl(
        std::move(reply),
        key.hasValue()
            ? folly::StringPiece(
                  reinterpret_cast<const char*>(key->data()), key->length())
            : folly::StringPiece());
    iovOut = iovs_;
    niovOut = iovsCount_;
    return true;
  }

  template <class Reply>
  bool prepare(
      Reply&& reply,
      const folly::Optional<folly::IOBuf>& /* key */,
      const struct iovec*& iovOut,
      size_t& niovOut,
      carbon::OtherThanT<Reply, carbon::GetLike<>> = 0) {
    prepareImpl(std::move(reply));
    iovOut = iovs_;
    niovOut = iovsCount_;
    return true;
  }

 private:
  // See comment in prepareImpl for McMetagetReply for explanation
  static constexpr size_t kMaxBufferLength = 100;

  static const size_t kMaxIovs = 16;
  struct iovec iovs_[kMaxIovs];
  size_t iovsCount_{0};
  char printBuffer_[kMaxBufferLength];
  // Used to keep alive the reply's IOBuf field (value, stats, etc.). For now,
  // replies have at most one IOBuf, so we only need one here. Note that one of
  // the iovs_ will point into the data managed by this IOBuf. A serialized
  // reply should not set iobuf_ more than once.
  // We also keep an auxiliary string for a similar purpose.
  folly::Optional<folly::IOBuf> iobuf_;
  folly::Optional<std::string> auxString_;

  void addString(folly::ByteRange range);
  void addString(folly::StringPiece str);

  template <class Arg1, class Arg2>
  void addStrings(Arg1&& arg1, Arg2&& arg2);
  template <class Arg, class... Args>
  void addStrings(Arg&& arg, Args&&... args);

  // Get-like ops
  void prepareImpl(McGetReply&& reply, folly::StringPiece key);
  void prepareImpl(McGetsReply&& reply, folly::StringPiece key);
  void prepareImpl(McMetagetReply&& reply, folly::StringPiece key);
  void prepareImpl(McLeaseGetReply&& reply, folly::StringPiece key);
  // Update-like ops
  void prepareUpdateLike(
      mc_res_t result,
      uint16_t errorCode,
      std::string&& message,
      const char* requestName);
  void prepareImpl(McSetReply&& reply);
  void prepareImpl(McAddReply&& reply);
  void prepareImpl(McReplaceReply&& reply);
  void prepareImpl(McAppendReply&& reply);
  void prepareImpl(McPrependReply&& reply);
  void prepareImpl(McCasReply&& reply);
  void prepareImpl(McLeaseSetReply&& reply);
  // Arithmetic-like ops
  void prepareArithmeticLike(
      mc_res_t result,
      const uint64_t delta,
      uint16_t errorCode,
      std::string&& message,
      const char* requestName);
  void prepareImpl(McIncrReply&& reply);
  void prepareImpl(McDecrReply&& reply);
  // Delete
  void prepareImpl(McDeleteReply&& reply);
  // Touch
  void prepareImpl(McTouchReply&& reply);
  // Version
  void prepareImpl(const McVersionReply& reply);
  void prepareImpl(McVersionReply&& reply);
  // Miscellaneous
  void prepareImpl(McStatsReply&&);
  void prepareImpl(McShutdownReply&&);
  void prepareImpl(McQuitReply&&) {} // always noreply
  void prepareImpl(McExecReply&&);
  void prepareImpl(McFlushReReply&&);
  void prepareImpl(McFlushAllReply&&);
  // Server and client error helper
  void handleError(mc_res_t result, uint16_t errorCode, std::string&& message);
  void handleUnexpected(mc_res_t result, const char* requestName);
};

}
}

#include "BinarySerialized-inl.h"
