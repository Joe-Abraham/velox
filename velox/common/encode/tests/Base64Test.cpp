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
#include "velox/common/base/Status.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::encoding {

class Base64Test : public ::testing::Test {};

TEST_F(Base64Test, fromBase64) {
  EXPECT_EQ("Hello, World!", Base64::decode("SGVsbG8sIFdvcmxkIQ=="));
  EXPECT_EQ(
      "Base64 encoding is fun.",
      Base64::decode("QmFzZTY0IGVuY29kaW5nIGlzIGZ1bi4="));
  EXPECT_EQ("Simple text", Base64::decode("U2ltcGxlIHRleHQ="));
  EXPECT_EQ("1234567890", Base64::decode("MTIzNDU2Nzg5MA=="));

  // Check encoded strings without padding
  EXPECT_EQ("Hello, World!", Base64::decode("SGVsbG8sIFdvcmxkIQ"));
  EXPECT_EQ(
      "Base64 encoding is fun.",
      Base64::decode("QmFzZTY0IGVuY29kaW5nIGlzIGZ1bi4"));
  EXPECT_EQ("Simple text", Base64::decode("U2ltcGxlIHRleHQ"));
  EXPECT_EQ("1234567890", Base64::decode("MTIzNDU2Nzg5MA"));
}

TEST_F(Base64Test, calculateDecodedSizeProperSize) {
  size_t encoded_size{0};
  size_t decoded_size{0};

  encoded_size = 20;
  Base64::calculateDecodedSize(
      "SGVsbG8sIFdvcmxkIQ==", encoded_size, decoded_size);
  EXPECT_EQ(18, encoded_size);
  EXPECT_EQ(13, decoded_size);

  encoded_size = 18;
  Base64::calculateDecodedSize(
      "SGVsbG8sIFdvcmxkIQ", encoded_size, decoded_size);
  EXPECT_EQ(18, encoded_size);
  EXPECT_EQ(13, decoded_size);

  encoded_size = 21;
  EXPECT_EQ(
      Status::UserError(
          "Base64::decode() - invalid input string: string length is not a multiple of 4."),
      Base64::calculateDecodedSize(
          "SGVsbG8sIFdvcmxkIQ===", encoded_size, decoded_size));

  encoded_size = 32;
  Base64::calculateDecodedSize(
      "QmFzZTY0IGVuY29kaW5nIGlzIGZ1bi4=", encoded_size, decoded_size);
  EXPECT_EQ(31, encoded_size);
  EXPECT_EQ(23, decoded_size);

  encoded_size = 31;
  Base64::calculateDecodedSize(
      "QmFzZTY0IGVuY29kaW5nIGlzIGZ1bi4", encoded_size, decoded_size);
  EXPECT_EQ(31, encoded_size);
  EXPECT_EQ(23, decoded_size);

  encoded_size = 16;
  Base64::calculateDecodedSize("MTIzNDU2Nzg5MA==", encoded_size, decoded_size);
  EXPECT_EQ(14, encoded_size);
  EXPECT_EQ(10, decoded_size);

  encoded_size = 14;
  Base64::calculateDecodedSize("MTIzNDU2Nzg5MA", encoded_size, decoded_size);
  EXPECT_EQ(14, encoded_size);
  EXPECT_EQ(10, decoded_size);
}

TEST_F(Base64Test, isPadded) {
  EXPECT_TRUE(Base64::isPadded("ABC=", 4));
  EXPECT_FALSE(Base64::isPadded("ABC", 3));
}

TEST_F(Base64Test, numPadding) {
  EXPECT_EQ(0, Base64::numPadding("ABC", 3));
  EXPECT_EQ(1, Base64::numPadding("ABC=", 4));
  EXPECT_EQ(2, Base64::numPadding("AB==", 4));
}
} // namespace facebook::velox::encoding
