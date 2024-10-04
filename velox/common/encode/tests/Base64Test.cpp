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
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::encoding {

class Base64Test : public ::testing::Test {};

TEST_F(Base64Test, fromBase64) {
  // Lambda function to reduce repetition in test cases
  auto checkBase64Decode = [](const std::string& expected,
                              const std::string& encoded) {
    EXPECT_EQ(expected, Base64::decode(folly::StringPiece(encoded)));
  };

  // Check encoded strings with padding
  checkBase64Decode("Hello, World!", "SGVsbG8sIFdvcmxkIQ==");
  checkBase64Decode(
      "Base64 encoding is fun.", "QmFzZTY0IGVuY29kaW5nIGlzIGZ1bi4=");
  checkBase64Decode("Simple text", "U2ltcGxlIHRleHQ=");
  checkBase64Decode("1234567890", "MTIzNDU2Nzg5MA==");

  // Check encoded strings without padding
  checkBase64Decode("Hello, World!", "SGVsbG8sIFdvcmxkIQ");
  checkBase64Decode(
      "Base64 encoding is fun.", "QmFzZTY0IGVuY29kaW5nIGlzIGZ1bi4");
  checkBase64Decode("Simple text", "U2ltcGxlIHRleHQ");
  checkBase64Decode("1234567890", "MTIzNDU2Nzg5MA");
}

TEST_F(Base64Test, isPadded) {
  EXPECT_TRUE(Base64::isPadded("ABC="));
  EXPECT_FALSE(Base64::isPadded("ABC"));
}

TEST_F(Base64Test, numPadding) {
  EXPECT_EQ(0, Base64::numPadding("ABC"));
  EXPECT_EQ(1, Base64::numPadding("ABC="));
  EXPECT_EQ(2, Base64::numPadding("AB=="));
}
} // namespace facebook::velox::encoding
