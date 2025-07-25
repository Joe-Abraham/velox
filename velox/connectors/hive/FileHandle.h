/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// A FileHandle is a File pointer plus some (optional, file-type-dependent)
// extra information for speeding up loading columnar data. For example, when
// we open a file we might build a hash map saying what region(s) on disk
// correspond to a given column in a given stripe.
//
// The FileHandle will normally be used in conjunction with a CachedFactory
// to speed up queries that hit the same files repeatedly; see the
// FileHandleCache and FileHandleFactory.

#pragma once

#include "velox/common/base/BitUtil.h"
#include "velox/common/caching/CachedFactory.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/File.h"
#include "velox/common/file/TokenProvider.h"
#include "velox/connectors/hive/FileProperties.h"

namespace facebook::velox {

// See the file comment.
struct FileHandle {
  std::shared_ptr<ReadFile> file;

  // Each time we make a new FileHandle we assign it a uuid and use that id as
  // the identifier in downstream data caching structures. This saves a lot of
  // memory compared to using the filename as the identifier.
  StringIdLease uuid;

  // Id for the group of files this belongs to, e.g. its
  // directory. Used for coarse granularity access tracking, for
  // example to decide placing on SSD.
  StringIdLease groupId;

  // We'll want to have a hash map here to record the identifier->byte range
  // mappings. Different formats may have different identifiers, so we may need
  // a union of maps. For example in orc you need 3 integers (I think, to be
  // confirmed with xldb): the row bundle, the node, and the sequence. For the
  // first diff we'll not include the map.
};

/// Estimates the memory usage of a FileHandle object.
struct FileHandleSizer {
  uint64_t operator()(const FileHandle& a);
};

struct FileHandleKey {
  std::string filename;
  std::shared_ptr<filesystems::TokenProvider> tokenProvider{nullptr};

  bool operator==(const FileHandleKey& other) const {
    if (filename != other.filename) {
      return false;
    }

    if (tokenProvider == other.tokenProvider) {
      return true;
    }

    if (!tokenProvider || !other.tokenProvider) {
      return false;
    }

    return tokenProvider->equals(*other.tokenProvider);
  }
};

} // namespace facebook::velox

namespace std {
template <>
struct hash<facebook::velox::FileHandleKey> {
  size_t operator()(const facebook::velox::FileHandleKey& key) const noexcept {
    size_t filenameHash = std::hash<std::string>()(key.filename);
    return key.tokenProvider ? facebook::velox::bits::hashMix(
                                   filenameHash, key.tokenProvider->hash())
                             : filenameHash;
  }
};
} // namespace std

namespace facebook::velox {
using FileHandleCache =
    SimpleLRUCache<facebook::velox::FileHandleKey, FileHandle>;

// Creates FileHandles via the Generator interface the CachedFactory requires.
class FileHandleGenerator {
 public:
  FileHandleGenerator() {}
  FileHandleGenerator(std::shared_ptr<const config::ConfigBase> properties)
      : properties_(std::move(properties)) {}
  std::unique_ptr<FileHandle> operator()(
      const FileHandleKey& filename,
      const FileProperties* properties,
      filesystems::File::IoStats* stats);

 private:
  const std::shared_ptr<const config::ConfigBase> properties_;
};

using FileHandleFactory = CachedFactory<
    FileHandleKey,
    FileHandle,
    FileHandleGenerator,
    FileProperties,
    filesystems::File::IoStats,
    FileHandleSizer>;

using FileHandleCachedPtr = CachedPtr<FileHandleKey, FileHandle>;

using FileHandleCacheStats = SimpleLRUCacheStats;

} // namespace facebook::velox
