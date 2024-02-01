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

#include "velox/common/encode/Base64.h"
#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::encoding {
class Base64Test : public ::testing::Test {};

TEST_F(Base64Test, fromBase64) {
  EXPECT_EQ(
      "Hello, World!",
      Base64::decode(folly::StringPiece("SGVsbG8sIFdvcmxkIQ==")));
  EXPECT_EQ(
      "Base64 encoding is fun.",
      Base64::decode(folly::StringPiece("QmFzZTY0IGVuY29kaW5nIGlzIGZ1bi4=")));
  EXPECT_EQ(
      "Simple text", Base64::decode(folly::StringPiece("U2ltcGxlIHRleHQ=")));
  EXPECT_EQ(
      "1234567890", Base64::decode(folly::StringPiece("MTIzNDU2Nzg5MA==")));

  // Check encoded strings without padding
  EXPECT_EQ(
      "Hello, World!",
      Base64::decode(folly::StringPiece("SGVsbG8sIFdvcmxkIQ")));
  EXPECT_EQ(
      "Base64 encoding is fun.",
      Base64::decode(folly::StringPiece("QmFzZTY0IGVuY29kaW5nIGlzIGZ1bi4")));
  EXPECT_EQ(
      "Simple text", Base64::decode(folly::StringPiece("U2ltcGxlIHRleHQ")));
  EXPECT_EQ("1234567890", Base64::decode(folly::StringPiece("MTIzNDU2Nzg5MA")));
}

struct TestCase {
  std::string inputBase64;
  size_t initialEncodedSize;
  size_t expectedDecodedSize;
  size_t expectedEncodedSizeAfter;
};

std::vector<TestCase> testCases{
    {"SGVsbG8sIFdvcmxkIQ==", 20, 13, 18},
    {"SGVsbG8sIFdvcmxkIQ", 18, 13, 18},
    {"QmFzZTY0IGVuY29kaW5nIGlzIGZ1bi4=", 32, 23, 31},
    {"QmFzZTY0IGVuY29kaW5nIGlzIGZ1bi4", 31, 23, 31},
    {"MTIzNDU2Nzg5MA==", 16, 10, 14},
    {"MTIzNDU2Nzg5MA", 14, 10, 14}};

TEST_F(Base64Test, calculateDecodedSizeProperSize) {
  for (const auto& testCase : testCases) {
    size_t encodedSize = testCase.initialEncodedSize;
    size_t decodedSize =
        Base64::calculateDecodedSize(testCase.inputBase64.c_str(), encodedSize);
    EXPECT_EQ(testCase.expectedDecodedSize, decodedSize);
    EXPECT_EQ(testCase.expectedEncodedSizeAfter, encodedSize);
  }
}

TEST_F(Base64Test, errorWhenDecodedStringPartiallyPadded) {
  size_t encoded_size = 21;
  EXPECT_THROW(
      Base64::calculateDecodedSize("SGVsbG8sIFdvcmxkIQ==", encoded_size),
      facebook::velox::encoding::EncoderException);
}

} // namespace facebook::velox::encoding
