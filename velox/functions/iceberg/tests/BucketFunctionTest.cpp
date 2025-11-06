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
#include "velox/functions/iceberg/BucketFunction.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/iceberg/tests/IcebergFunctionBaseTest.h"

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace facebook::velox::functions::iceberg {
namespace {

class BucketFunctionTest
    : public functions::iceberg::test::IcebergFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<int32_t> bucket(
      std::optional<T> value,
      std::optional<int32_t> numBuckets) {
    return evaluateOnce<int32_t>("bucket(c0, c1)", value, numBuckets);
  }

  template <typename T>
  std::optional<int32_t> bucket(
      const TypePtr& type,
      std::optional<T> value,
      std::optional<int32_t> numBuckets) {
    return evaluateOnce<int32_t>(
        "bucket(c0, c1)", {type, INTEGER()}, value, numBuckets);
  }
};

TEST_F(BucketFunctionTest, integerTypes) {
  EXPECT_EQ(bucket<int32_t>(8, 10), 3);
  EXPECT_EQ(bucket<int32_t>(42, 10), 6);
  EXPECT_EQ(bucket<int32_t>(34, 100), 79);
  EXPECT_EQ(bucket<int32_t>(INT_MAX, 1000), 606);
  EXPECT_EQ(bucket<int32_t>(INT_MIN, 1000), 856);
  EXPECT_EQ(bucket<int64_t>(8, 10), 3);
  EXPECT_EQ(bucket<int64_t>(42, 10), 6);
  EXPECT_EQ(bucket<int64_t>(-34, 100), 97);
  EXPECT_EQ(bucket<int64_t>(-1, 2), 0);
  VELOX_ASSERT_THROW(
      bucket<int64_t>(34, 0),
      "Reason: (0 vs. 0) Invalid number of buckets.\nExpression: numBuckets <= 0\n");
  VELOX_ASSERT_THROW(
      bucket<int64_t>(34, -3),
      "Reason: (-3 vs. 0) Invalid number of buckets.\nExpression: numBuckets <= 0\n");
}

TEST_F(BucketFunctionTest, string) {
  EXPECT_EQ(bucket<std::string>("abcdefg", 5), 4);
  EXPECT_EQ(bucket<std::string>("abc", 128), 122);
  EXPECT_EQ(bucket<std::string>("abcd", 128), 106);
  EXPECT_EQ(bucket<std::string>("abcde", 64), 54);
  EXPECT_EQ(bucket<std::string>("测试", 12), 8);
  EXPECT_EQ(bucket<std::string>("测试raul试测", 16), 1);
  EXPECT_EQ(bucket<std::string>("", 16), 0);
  EXPECT_EQ(bucket<std::string>("Товары", 16), 10);
  EXPECT_EQ(bucket<std::string>("😀", 120), 58);
  VELOX_ASSERT_THROW(
      bucket<std::string>("abc", 0),
      "Reason: (0 vs. 0) Invalid number of buckets.\nExpression: numBuckets <= 0\n");
  VELOX_ASSERT_THROW(
      bucket<std::string>("abc", -3),
      "Reason: (-3 vs. 0) Invalid number of buckets.\nExpression: numBuckets <= 0\n");
}

TEST_F(BucketFunctionTest, binary) {
  EXPECT_EQ(bucket<StringView>(VARBINARY(), "abc", 128), 122);
  EXPECT_EQ(bucket<StringView>(VARBINARY(), "abcdefg", 5), 4);
}

TEST_F(BucketFunctionTest, timestamp) {
  EXPECT_EQ(bucket<Timestamp>(Timestamp(1633046400, 0), 20), 17);
  EXPECT_EQ(bucket<Timestamp>(Timestamp(0, 123456789), 20), 0);
  EXPECT_EQ(bucket<Timestamp>(Timestamp(-62167219200, 0), 20), 5);
  VELOX_ASSERT_THROW(
      bucket<Timestamp>(Timestamp(-62167219200, 0), 0),
      "Reason: (0 vs. 0) Invalid number of buckets.\nExpression: numBuckets <= 0\n");
  VELOX_ASSERT_THROW(
      bucket<Timestamp>(Timestamp(-62167219200, 0), -3),
      "Reason: (-3 vs. 0) Invalid number of buckets.\nExpression: numBuckets <= 0\n");
}

TEST_F(BucketFunctionTest, date) {
  EXPECT_EQ(bucket<int32_t>(DATE(), 8, 10), 3);
  EXPECT_EQ(bucket<int32_t>(DATE(), 42, 10), 6);
}

TEST_F(BucketFunctionTest, decimal) {
  EXPECT_EQ(bucket<int64_t>(DECIMAL(9, 2), 1234, 64), 56);
  EXPECT_EQ(bucket<int64_t>(DECIMAL(9, 2), 1230, 18), 13);
  EXPECT_EQ(bucket<int64_t>(DECIMAL(9, 2), 12999, 16), 2);
  EXPECT_EQ(bucket<int64_t>(DECIMAL(9, 2), 5, 128), 85);
  EXPECT_EQ(bucket<int64_t>(DECIMAL(9, 2), 5, 18), 3);

  EXPECT_EQ(bucket<int128_t>(DECIMAL(38, 2), 12, 10), 7);
  EXPECT_EQ(bucket<int128_t>(DECIMAL(38, 2), 0, 10), 7);
  EXPECT_EQ(bucket<int128_t>(DECIMAL(38, 2), 234, 10), 6);
  EXPECT_EQ(
      bucket<int128_t>(DECIMAL(38, 2), DecimalUtil::kLongDecimalMax, 10), 4);
}

} // namespace
} // namespace facebook::velox::functions::iceberg
