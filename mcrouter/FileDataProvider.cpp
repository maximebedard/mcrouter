/*
 *  Copyright (c) 2014-present, Facebook, Inc.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */
#include "FileDataProvider.h"

#include <libfswatch/c++/monitor.hpp>

#include <boost/filesystem/convenience.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/system/error_code.hpp>

#include <folly/FileUtil.h>
#include <folly/Format.h>

using boost::filesystem::complete;
using boost::filesystem::path;
using boost::filesystem::read_symlink;

namespace facebook {
namespace memcache {
namespace mcrouter {

FileDataProvider::FileDataProvider(std::string filePath)
    : filePath_(std::move(filePath)) {
  if (filePath_.empty()) {
    throw std::runtime_error("File path empty");
  }

  std::vector<std::string> paths { filePath_ };
  auto monitor_ = fsw::monitor_factory::create_monitor(
    fsw_monitor_type::system_default_monitor_type,
    paths,
    [](const std::vector<fsw::event> &events, void *context){
      auto c = static_cast<FileDataProvider::Context*>(context);
      std::lock_guard<std::mutex> lg(c->mutex_);
      c->has_update_ = true;
    },
    &ctx_
  );
  monitor_->set_follow_symlinks(true);
  monitor_->start();
}

std::string FileDataProvider::load() const {
  std::string result;
  if (!folly::readFile(filePath_.data(), result)) {
    throw std::runtime_error(
        folly::format("Can not read file '{}'", filePath_.data()).str());
  }
  return result;
}

bool FileDataProvider::hasUpdate() {
  std::lock_guard<std::mutex> lg(ctx_.mutex_);
  if (ctx_.has_update_) {
    ctx_.has_update_ = false;
    return true;
  }
  return false;
}

}
}
} // facebook::memcache::mcrouter
