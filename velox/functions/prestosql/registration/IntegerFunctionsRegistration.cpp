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

#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/IntegerFunctions.h"

namespace facebook::velox::functions {
namespace {

void registerSimpleFunctions(const std::string& prefix) {
  registerFunction<XxHash64BigIntFunction, int64_t, int64_t>(
      {prefix + "xxhash64_internal"});
  registerFunction<XxHash64IntegerFunction, int64_t, int32_t>(
      {prefix + "xxhash64_internal"});
  registerFunction<XxHash64SmallIntFunction, int64_t, int16_t>(
      {prefix + "xxhash64_internal"});
  registerFunction<XxHash64TinyIntFunction, int64_t, int8_t>(
      {prefix + "xxhash64_internal"});

  registerFunction<CombineHashFunction, int64_t, int64_t, int64_t>(
      {prefix + "combine_hash_internal"});
}
} // namespace

void registerIntegerFunctions(const std::string& prefix) {
  registerSimpleFunctions(prefix);
}

} // namespace facebook::velox::functions
