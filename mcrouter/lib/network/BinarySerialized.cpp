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

namespace facebook {
namespace memcache {

size_t BinarySerializedMessage::getSize() const {
  return iovsTotalLen_;
}

void BinarySerializedMessage::addString(folly::ByteRange range) {
  assert(iovsCount_ < kMaxIovs);
  auto bufLen = range.size();
  iovs_[iovsCount_].iov_base = const_cast<unsigned char*>(range.begin());
  iovs_[iovsCount_].iov_len = bufLen;
  ++iovsCount_;
  iovsTotalLen_ += bufLen;
}

void BinarySerializedMessage::addString(folly::StringPiece str) {
  // cause implicit conversion.
  addString(folly::ByteRange(str));
}

template <class Request>
void BinarySerializedMessage::keyValueRequestCommon(
    folly::StringPiece prefix,
    const Request& request) {

}

// Get-like ops.
void BinarySerializedMessage::prepareImpl(const McGetRequest& request) {
}

void BinarySerializedMessage::prepareImpl(const McGetsRequest& request) {
}

void BinarySerializedMessage::prepareImpl(const McMetagetRequest& request) {
}

void BinarySerializedMessage::prepareImpl(const McLeaseGetRequest& request) {
}

// Update-like ops.
void BinarySerializedMessage::prepareImpl(const McSetRequest& request) {
}

void BinarySerializedMessage::prepareImpl(const McAddRequest& request) {
}

void BinarySerializedMessage::prepareImpl(const McReplaceRequest& request) {
}

void BinarySerializedMessage::prepareImpl(const McAppendRequest& request) {
}

void BinarySerializedMessage::prepareImpl(const McPrependRequest& request) {
}

void BinarySerializedMessage::prepareImpl(const McCasRequest& request) {
}

void BinarySerializedMessage::prepareImpl(const McLeaseSetRequest& request) {
}

// Arithmetic ops.
void BinarySerializedMessage::prepareImpl(const McIncrRequest& request) {
}

void BinarySerializedMessage::prepareImpl(const McDecrRequest& request) {
}

// Delete op.
void BinarySerializedMessage::prepareImpl(const McDeleteRequest& request) {
}

// Touch op.
void BinarySerializedMessage::prepareImpl(const McTouchRequest& request) {
}

// Version op.
void BinarySerializedMessage::prepareImpl(const McVersionRequest&) {
}

// FlushAll op.

void BinarySerializedMessage::prepareImpl(const McFlushAllRequest& request) {
}

void BinarySerializedMessage::clear() {
  iovsCount_ = 0;
  iobuf_.clear();
  auxString_.clear();
}
//
//void BinarySerializedReply::addString(folly::ByteRange range) {
//  assert(iovsCount_ < kMaxIovs);
//  iovs_[iovsCount_].iov_base = const_cast<unsigned char*>(range.begin());
//  iovs_[iovsCount_].iov_len = range.size();
//  ++iovsCount_;
//}
//
//void BinarySerializedReply::addString(folly::StringPiece str) {
//  // cause implicit conversion.
//  addString(folly::ByteRange(str));
//}
//
//void BinarySerializedReply::handleError(
//    mc_res_t result,
//    uint16_t errorCode,
//    std::string&& message) {
//}
//
//void BinarySerializedReply::handleUnexpected(
//    mc_res_t result,
//    const char* requestName) {
//}
//
//// Get-like ops
//void BinarySerializedReply::prepareImpl(
//    McGetReply&& reply,
//    folly::StringPiece key) {
//}
//
//void BinarySerializedReply::prepareImpl(
//    McGetsReply&& reply,
//    folly::StringPiece key) {
//}
//
//void BinarySerializedReply::prepareImpl(
//    McMetagetReply&& reply,
//    folly::StringPiece key) {
//}
//
//void BinarySerializedReply::prepareImpl(
//    McLeaseGetReply&& reply,
//    folly::StringPiece key) {
//}
//
//// Update-like ops
//void BinarySerializedReply::prepareUpdateLike(
//    mc_res_t result,
//    uint16_t errorCode,
//    std::string&& message,
//    const char* requestName) {
//}
//
//void BinarySerializedReply::prepareImpl(McSetReply&& reply) {
//}
//
//void BinarySerializedReply::prepareImpl(McAddReply&& reply) {
//}
//
//void BinarySerializedReply::prepareImpl(McReplaceReply&& reply) {
//}
//
//void BinarySerializedReply::prepareImpl(McAppendReply&& reply) {
//}
//
//void BinarySerializedReply::prepareImpl(McPrependReply&& reply) {
//}
//
//void BinarySerializedReply::prepareImpl(McCasReply&& reply) {
//}
//
//void BinarySerializedReply::prepareImpl(McLeaseSetReply&& reply) {
//}
//
//void BinarySerializedReply::prepareArithmeticLike(
//    mc_res_t result,
//    const uint64_t delta,
//    uint16_t errorCode,
//    std::string&& message,
//    const char* requestName) {
//}
//
//// Arithmetic-like ops
//void BinarySerializedReply::prepareImpl(McIncrReply&& reply) {
//}
//
//void BinarySerializedReply::prepareImpl(McDecrReply&& reply) {
//}
//
//// Delete
//void BinarySerializedReply::prepareImpl(McDeleteReply&& reply) {
//}
//
//// Touch
//void BinarySerializedReply::prepareImpl(McTouchReply&& reply) {
//}
//
//// Version
//void BinarySerializedReply::prepareImpl(McVersionReply&& reply) {
//}
//
//// Stats
//void BinarySerializedReply::prepareImpl(McStatsReply&& reply) {
//}
//
//// FlushAll
//void BinarySerializedReply::prepareImpl(McFlushAllReply&& reply) {
//}
//
//// FlushRe
//void BinarySerializedReply::prepareImpl(McFlushReReply&& reply) {
//}
//
//// Exec
//void BinarySerializedReply::prepareImpl(McExecReply&& reply) {
//}
//
//// Shutdown
//void BinarySerializedReply::prepareImpl(McShutdownReply&& reply) {
//}
}
} // facebook::memcache
