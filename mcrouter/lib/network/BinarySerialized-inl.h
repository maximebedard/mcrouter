/*
 *  Copyright (c) 2015-present, Facebook, Inc.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */
namespace facebook {
namespace memcache {

struct BinarySerializedMessage::PrepareImplWrapper {
 template <class Request>
 using RequestT =
     decltype(std::declval<BinarySerializedMessage>().prepareImpl(
         std::declval<const Request&>()));

 template <class Request>
 typename std::enable_if<
     std::is_same<RequestT<Request>, std::false_type>::value,
     bool>::type static prepare(BinarySerializedMessage&, const Request&) {
   return false;
 }

 template <class Request>
 typename std::enable_if<
     std::is_same<RequestT<Request>, void>::value,
     bool>::type static prepare(BinarySerializedMessage& s, const Request& request) {
   s.prepareImpl(request);
   return true;
 }

 // temporarily stub out repliess
 template <class Reply>
 using ReplyT =
     decltype(std::declval<BinarySerializedMessage>().prepareImpl(
         std::declval<Reply&&>()));

 template <class Reply>
 typename std::enable_if<
     std::is_same<ReplyT<Reply>, std::false_type>::value,
     bool>::type static prepare(BinarySerializedMessage&, Reply&&) {
   return false;
 }

 template <class Reply>
 typename std::enable_if<
     std::is_same<ReplyT<Reply>, void>::value,
     bool>::type static prepare(BinarySerializedMessage& s, const Reply&& reply) {
   s.prepareImpl(reply);
   return true;
 }
};

template <class Request>
bool BinarySerializedMessage::prepare(
    const Request& request,
	const struct iovec*& iovOut,
    size_t& niovOut) {
  iovsCount_ = 0;
  auto r = PrepareImplWrapper::prepare(*this, request);
  iovOut = iovs_;
  niovOut = iovsCount_;
  return r;
}

template <class Reply>
bool BinarySerializedMessage::prepare(
    Reply&& reply,
    size_t reqId,
	const struct iovec*& iovOut,
    size_t& niovOut) {
  iovsCount_ = 0;
  auto r = PrepareImplWrapper::prepare(*this, reply);
  iovOut = iovs_;
  niovOut = iovsCount_;
  return r;
}

}
} // facebook::memcache
