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

void BinarySerializedReply::clear() {
  iovsCount_ = 0;
  iobuf_.clear();
  auxString_.clear();
}

void BinarySerializedReply::addString(folly::ByteRange range) {
  assert(iovsCount_ < kMaxIovs);
  iovs_[iovsCount_].iov_base = const_cast<unsigned char*>(range.begin());
  iovs_[iovsCount_].iov_len = range.size();
  ++iovsCount_;
}

void BinarySerializedReply::addString(folly::StringPiece str) {
  // cause implicit conversion.
  addString(folly::ByteRange(str));
}

void BinarySerializedReply::handleError(
    mc_res_t result,
    uint16_t errorCode,
    std::string&& message) {
  assert(isErrorResult(result));

  if (!message.empty()) {
    if (result == mc_res_client_error) {
      addString("CLIENT_ERROR ");
    } else {
      addString("SERVER_ERROR ");
    }
    if (errorCode != 0) {
      const auto len =
          snprintf(printBuffer_, kMaxBufferLength, "%d ", errorCode);
      assert(len > 0);
      assert(static_cast<size_t>(len) < kMaxBufferLength);
      addString(folly::StringPiece(printBuffer_, static_cast<size_t>(len)));
    }
    auxString_ = std::move(message);
    addStrings(*auxString_, "\r\n");
  } else {
    addString(mc_res_to_response_string(result));
  }
}

void BinarySerializedReply::handleUnexpected(
    mc_res_t result,
    const char* requestName) {
  assert(iovsCount_ == 0);

  // Note: this is not totally compatible with the old way of handling
  // unexpected behavior in mc_ascii_response_write_iovs()
  const auto len = snprintf(
      printBuffer_,
      kMaxBufferLength,
      "SERVER_ERROR unexpected result %s (%d) for %s\r\n",
      mc_res_to_string(result),
      result,
      requestName);
  assert(len > 0);
  assert(static_cast<size_t>(len) < kMaxBufferLength);
  addString(folly::StringPiece(printBuffer_, static_cast<size_t>(len)));
}

// Get-like ops
void BinarySerializedReply::prepareImpl(
    McGetReply&& reply,
    folly::StringPiece key) {
  if (isHitResult(reply.result())) {
    if (key.empty()) {
      // Multi-op hack: if key is empty, this is the END context
      if (isErrorResult(reply.result())) {
        handleError(
            reply.result(),
            reply.appSpecificErrorCode(),
            std::move(reply.message()));
      }
      addString("END\r\n");
    } else {
      const auto valueStr = coalesceAndGetRange(reply.value());

      const auto len = snprintf(
          printBuffer_,
          kMaxBufferLength,
          " %lu %zu\r\n",
          reply.flags(),
          valueStr.size());
      assert(len > 0);
      assert(static_cast<size_t>(len) < kMaxBufferLength);

      addStrings(
          "VALUE ",
          key,
          folly::StringPiece(printBuffer_, static_cast<size_t>(len)));
      assert(!iobuf_.hasValue());
      // value was coalesced in coalesceAndGetRange()
      iobuf_ = std::move(reply.value());
      addStrings(valueStr, "\r\n");
    }
  } else if (isErrorResult(reply.result())) {
    handleError(
        reply.result(),
        reply.appSpecificErrorCode(),
        std::move(reply.message()));
  } else {
    handleUnexpected(reply.result(), "get");
  }
}

void BinarySerializedReply::prepareImpl(
    McGetsReply&& reply,
    folly::StringPiece key) {
  if (isHitResult(reply.result())) {
    const auto valueStr = coalesceAndGetRange(reply.value());
    const auto len = snprintf(
        printBuffer_,
        kMaxBufferLength,
        " %lu %zu %lu\r\n",
        reply.flags(),
        valueStr.size(),
        reply.casToken());
    assert(len > 0);
    assert(static_cast<size_t>(len) < kMaxBufferLength);

    addStrings(
        "VALUE ",
        key,
        folly::StringPiece(printBuffer_, static_cast<size_t>(len)));
    assert(!iobuf_.hasValue());
    // value was coalesced in coalescedAndGetRange()
    iobuf_ = std::move(reply.value());
    addStrings(valueStr, "\r\n");
  } else if (isErrorResult(reply.result())) {
    handleError(
        reply.result(),
        reply.appSpecificErrorCode(),
        std::move(reply.message()));
  } else {
    handleUnexpected(reply.result(), "gets");
  }
}

void BinarySerializedReply::prepareImpl(
    McMetagetReply&& reply,
    folly::StringPiece key) {
  /**
   * META key age: (unknown|\d+); exptime: \d+;
   * from: (\d+\.\d+\.\d+\.\d+|unknown); is_transient: (1|0)\r\n
   *
   * age is at most 11 characters, with - sign.
   * exptime is at most 10 characters.
   * IP6 address is at most 39 characters.
   * To be safe, we set kMaxBufferLength = 100 bytes.
   */
  if (reply.result() == mc_res_found) {
    // age
    std::string ageStr("unknown");
    if (reply.age() != -1) {
      ageStr = folly::to<std::string>(reply.age());
    }
    // exptime
    const auto exptimeStr = folly::to<std::string>(reply.exptime());
    // from
    std::string fromStr("unknown");
    if (!reply.ipAddress().empty()) { // assume valid IP
      fromStr = reply.ipAddress();
    }

    const auto len = snprintf(
        printBuffer_,
        kMaxBufferLength,
        "%s; exptime: %s; from: %s",
        ageStr.data(),
        exptimeStr.data(),
        fromStr.data());
    assert(len > 0);
    assert(static_cast<size_t>(len) < kMaxBufferLength);

    addStrings(
        "META ",
        key,
        " age: ",
        folly::StringPiece(printBuffer_, static_cast<size_t>(len)),
        "; is_transient: 0\r\n");
  } else if (isErrorResult(reply.result())) {
    handleError(
        reply.result(),
        reply.appSpecificErrorCode(),
        std::move(reply.message()));
  } else {
    handleUnexpected(reply.result(), "metaget");
  }
}

void BinarySerializedReply::prepareImpl(
    McLeaseGetReply&& reply,
    folly::StringPiece key) {
  const auto valueStr = coalesceAndGetRange(reply.value());

  if (reply.result() == mc_res_found) {
    const auto len = snprintf(
        printBuffer_,
        kMaxBufferLength,
        " %lu %zu\r\n",
        reply.flags(),
        valueStr.size());
    assert(len > 0);
    assert(static_cast<size_t>(len) < kMaxBufferLength);

    addStrings(
        "VALUE ",
        key,
        folly::StringPiece(printBuffer_, static_cast<size_t>(len)));
    assert(!iobuf_.hasValue());
    // value was coalesced in coalescedAndGetRange()
    iobuf_ = std::move(reply.value());
    addStrings(valueStr, "\r\n");
  } else if (reply.result() == mc_res_notfound) {
    const auto len = snprintf(
        printBuffer_,
        kMaxBufferLength,
        " %lu %lu %zu\r\n",
        reply.leaseToken(),
        reply.flags(),
        valueStr.size());
    addStrings(
        "LVALUE ",
        key,
        folly::StringPiece(printBuffer_, static_cast<size_t>(len)));
    iobuf_ = std::move(reply.value());
    addStrings(valueStr, "\r\n");
  } else if (reply.result() == mc_res_notfoundhot) {
    addString("NOT_FOUND_HOT\r\n");
  } else if (isErrorResult(reply.result())) {
    LOG(ERROR) << "Got reply result " << reply.result();
    handleError(
        reply.result(),
        reply.appSpecificErrorCode(),
        std::move(reply.message()));
  } else {
    LOG(ERROR) << "Got unexpected reply result " << reply.result();
    handleUnexpected(reply.result(), "lease-get");
  }
}

// Update-like ops
void BinarySerializedReply::prepareUpdateLike(
    mc_res_t result,
    uint16_t errorCode,
    std::string&& message,
    const char* requestName) {
  if (isErrorResult(result)) {
    handleError(result, errorCode, std::move(message));
    return;
  }

  if (UNLIKELY(result == mc_res_ok)) {
    addString(mc_res_to_response_string(mc_res_stored));
    return;
  }

  switch (result) {
    case mc_res_stored:
    case mc_res_stalestored:
    case mc_res_found:
    case mc_res_notstored:
    case mc_res_notfound:
    case mc_res_exists:
      addString(mc_res_to_response_string(result));
      break;

    default:
      handleUnexpected(result, requestName);
      break;
  }
}

void BinarySerializedReply::prepareImpl(McSetReply&& reply) {
  prepareUpdateLike(
      reply.result(),
      reply.appSpecificErrorCode(),
      std::move(reply.message()),
      "set");
}

void BinarySerializedReply::prepareImpl(McAddReply&& reply) {
  prepareUpdateLike(
      reply.result(),
      reply.appSpecificErrorCode(),
      std::move(reply.message()),
      "add");
}

void BinarySerializedReply::prepareImpl(McReplaceReply&& reply) {
  prepareUpdateLike(
      reply.result(),
      reply.appSpecificErrorCode(),
      std::move(reply.message()),
      "replace");
}

void BinarySerializedReply::prepareImpl(McAppendReply&& reply) {
  prepareUpdateLike(
      reply.result(),
      reply.appSpecificErrorCode(),
      std::move(reply.message()),
      "append");
}

void BinarySerializedReply::prepareImpl(McPrependReply&& reply) {
  prepareUpdateLike(
      reply.result(),
      reply.appSpecificErrorCode(),
      std::move(reply.message()),
      "prepend");
}

void BinarySerializedReply::prepareImpl(McCasReply&& reply) {
  prepareUpdateLike(
      reply.result(),
      reply.appSpecificErrorCode(),
      std::move(reply.message()),
      "cas");
}

void BinarySerializedReply::prepareImpl(McLeaseSetReply&& reply) {
  prepareUpdateLike(
      reply.result(),
      reply.appSpecificErrorCode(),
      std::move(reply.message()),
      "lease-set");
}

void BinarySerializedReply::prepareArithmeticLike(
    mc_res_t result,
    const uint64_t delta,
    uint16_t errorCode,
    std::string&& message,
    const char* requestName) {
  if (isStoredResult(result)) {
    const auto len = snprintf(printBuffer_, kMaxBufferLength, "%lu\r\n", delta);
    assert(len > 0);
    assert(static_cast<size_t>(len) < kMaxBufferLength);
    addString(folly::StringPiece(printBuffer_, static_cast<size_t>(len)));
  } else if (isMissResult(result)) {
    addString("NOT_FOUND\r\n");
  } else if (isErrorResult(result)) {
    handleError(result, errorCode, std::move(message));
  } else {
    handleUnexpected(result, requestName);
  }
}

// Arithmetic-like ops
void BinarySerializedReply::prepareImpl(McIncrReply&& reply) {
  prepareArithmeticLike(
      reply.result(),
      reply.delta(),
      reply.appSpecificErrorCode(),
      std::move(reply.message()),
      "incr");
}

void BinarySerializedReply::prepareImpl(McDecrReply&& reply) {
  prepareArithmeticLike(
      reply.result(),
      reply.delta(),
      reply.appSpecificErrorCode(),
      std::move(reply.message()),
      "decr");
}

// Delete
void BinarySerializedReply::prepareImpl(McDeleteReply&& reply) {
  if (reply.result() == mc_res_deleted) {
    addString("DELETED\r\n");
  } else if (reply.result() == mc_res_notfound) {
    addString("NOT_FOUND\r\n");
  } else if (isErrorResult(reply.result())) {
    handleError(
        reply.result(),
        reply.appSpecificErrorCode(),
        std::move(reply.message()));
  } else {
    handleUnexpected(reply.result(), "delete");
  }
}

// Touch
void BinarySerializedReply::prepareImpl(McTouchReply&& reply) {
  if (reply.result() == mc_res_touched) {
    addString("TOUCHED\r\n");
  } else if (reply.result() == mc_res_notfound) {
    addString("NOT_FOUND\r\n");
  } else if (isErrorResult(reply.result())) {
    handleError(
        reply.result(),
        reply.appSpecificErrorCode(),
        std::move(reply.message()));
  } else {
    handleUnexpected(reply.result(), "touch");
  }
}

// Version
void BinarySerializedReply::prepareImpl(McVersionReply&& reply) {
  if (reply.result() == mc_res_ok) {
    // TODO(jmswen) Do something sane when version is empty
    addString("VERSION ");
    if (!reply.value().empty()) {
      const auto valueStr = coalesceAndGetRange(reply.value());
      assert(!iobuf_.hasValue());
      // value was coalesced in coalesceAndGetRange()
      iobuf_ = std::move(reply.value());
      addString(valueStr);
    }
    addString("\r\n");
  } else if (isErrorResult(reply.result())) {
    handleError(
        reply.result(),
        reply.appSpecificErrorCode(),
        std::move(reply.message()));
  } else {
    handleUnexpected(reply.result(), "version");
  }
}

// Stats
void BinarySerializedReply::prepareImpl(McStatsReply&& reply) {
  if (reply.result() == mc_res_ok) {
    if (!reply.stats().empty()) {
      auxString_ = folly::join("\r\n", reply.stats());
      addStrings(*auxString_, "\r\n");
    }
    addString("END\r\n");
  } else if (isErrorResult(reply.result())) {
    handleError(
        reply.result(),
        reply.appSpecificErrorCode(),
        std::move(reply.message()));
  } else {
    handleUnexpected(reply.result(), "stats");
  }
}

// FlushAll
void BinarySerializedReply::prepareImpl(McFlushAllReply&& reply) {
  if (isErrorResult(reply.result())) {
    handleError(
        reply.result(),
        reply.appSpecificErrorCode(),
        std::move(reply.message()));
  } else { // Don't handleUnexpected(), just return OK
    addString("OK\r\n");
  }
}

// FlushRe
void BinarySerializedReply::prepareImpl(McFlushReReply&& reply) {
  if (isErrorResult(reply.result())) {
    handleError(
        reply.result(),
        reply.appSpecificErrorCode(),
        std::move(reply.message()));
  } else { // Don't handleUnexpected(), just return OK
    addString("OK\r\n");
  }
}

// Exec
void BinarySerializedReply::prepareImpl(McExecReply&& reply) {
  if (reply.result() == mc_res_ok) {
    if (!reply.response().empty()) {
      auxString_ = std::move(reply.response());
      addStrings(*auxString_, "\r\n");
    } else {
      addString("OK\r\n");
    }
  } else if (isErrorResult(reply.result())) {
    handleError(
        reply.result(),
        reply.appSpecificErrorCode(),
        std::move(reply.message()));
  } else {
    handleUnexpected(reply.result(), "exec");
  }
}

// Shutdown
void BinarySerializedReply::prepareImpl(McShutdownReply&& reply) {
  if (reply.result() == mc_res_ok) {
    addString("OK\r\n");
  } else if (isErrorResult(reply.result())) {
    handleError(
        reply.result(),
        reply.appSpecificErrorCode(),
        std::move(reply.message()));
  } else {
    handleUnexpected(reply.result(), "shutdown");
  }
}




}
}
