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

#pragma once

#include "velox/vector/BaseVector.h"

namespace facebook::velox::dwio::common {

template <typename T>
inline void ensureCapacity(
    BufferPtr& data,
    size_t numElements,
    velox::memory::MemoryPool* pool,
    bool preserveOldData = false,
    bool clearBits = false) {
  size_t oldSize = 0;
  size_t newCapacity = BaseVector::byteSize<T>(numElements);
  if (!data) {
    data = AlignedBuffer::allocate<T>(numElements, pool);
  } else {
    oldSize = data->size();
    if (!data->isMutable() || data->capacity() < newCapacity) {
      auto newData = AlignedBuffer::allocate<T>(numElements, pool);
      if (preserveOldData) {
        std::memcpy(
            newData->template asMutable<uint8_t>(),
            data->as<uint8_t>(),
            oldSize);
      }
      data = newData;
    }
  }

  if (clearBits && newCapacity > oldSize) {
    std::memset(
        (void*)(data->asMutable<int8_t>() + oldSize),
        0L,
        newCapacity - oldSize);
  }
}

template <typename T>
inline T* resetIfWrongVectorType(VectorPtr& result) {
  if (result) {
    auto casted = result->as<T>();
    // We only expect vector to be used by a single thread.
    if (casted && result.use_count() == 1) {
      return casted;
    }
    result.reset();
  }
  return nullptr;
}

template <typename... T>
inline void resetIfNotWritable(VectorPtr& result, T&... buffer) {
  // The result vector and the buffer both hold reference, so refCount is at
  // least 2
  auto resetIfShared = [](auto& buffer) {
    const bool reset = buffer->refCount() > 2;
    if (reset) {
      buffer.reset();
    }
    return reset;
  };

  if ((... | resetIfShared(buffer))) {
    result.reset();
  }
}

} // namespace facebook::velox::dwio::common
