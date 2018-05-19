/*
 *  Copyright (c) 2015-present, Facebook, Inc.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */
namespace facebook {
namespace memcache {

template <class Arg1, class Arg2>
void BinarySerializedReply::addStrings(Arg1&& arg1, Arg2&& arg2) {
  addString(std::forward<Arg1>(arg1));
  addString(std::forward<Arg2>(arg2));
}

template <class Arg, class... Args>
void BinarySerializedReply::addStrings(Arg&& arg, Args&&... args) {
  addString(std::forward<Arg>(arg));
  addStrings(std::forward<Args>(args)...);
}

} // memcache
} // facebook
