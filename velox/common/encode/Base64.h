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

#include <exception>
#include <map>
#include <string>

#include <folly/Range.h>
#include <folly/io/IOBuf.h>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::encoding {

const size_t kCharsetSize = 64;
const size_t kReverseIndexSize = 256;

/// Character set used for encoding purposes.
/// Contains specific characters that form the encoding scheme.
using Charset = std::array<char, kCharsetSize>;

/// Reverse lookup table for decoding purposes.
/// Maps each possible encoded character to its corresponding numeric value
/// within the encoding base.
using ReverseIndex = std::array<uint8_t, kReverseIndexSize>;

/// Performs a reverse lookup in the reverse index to retrieve the original
/// index of a character in the base.
inline uint8_t
baseReverseLookup(int base, char p, const ReverseIndex& reverseIndex) {
  auto curr = reverseIndex[(uint8_t)p];
  if (curr >= base) {
    VELOX_USER_FAIL("decode() - invalid input string: invalid characters");
  }
  return curr;
}

class Base64 {
 public:
  /// Padding character used in encoding.
  const static char kPadding = '=';

  /// Returns encoded size for the input of the specified size.
  static size_t calculateEncodedSize(size_t size, bool withPadding = true);

  /// Encodes the specified number of characters from the 'data' and writes the
  /// result to the 'output'. The output must have enough space, e.g. as
  /// returned by the calculateEncodedSize().
  static void encode(const char* data, size_t size, char* output);

  /// Encodes the specified number of characters from the 'data'.
  static std::string encode(const char* data, size_t len);

  /// Encodes the specified text.
  static std::string encode(folly::StringPiece text);

  /// Encodes the specified IOBuf data.
  static std::string encode(const folly::IOBuf* text);

  /// Returns the actual size of the decoded data. Will also remove the padding
  /// length from the input data 'size'.
  static size_t calculateDecodedSize(const char* data, size_t& size);

  /// Decodes the specified number of characters from the 'data' and writes the
  /// result to the 'output'. The output must have enough space, e.g. as
  /// returned by the calculateDecodedSize().
  static void decode(const char* data, size_t size, char* output);

  /// Decodes the specified payload and writes the result to the 'output'.
  static void decode(
      const std::pair<const char*, int32_t>& payload,
      std::string& output);

  /// Decodes the specified encoded text.
  static std::string decode(folly::StringPiece encoded);

  /// Decodes the specified number of characters from the 'src' and writes the
  /// result to the 'dst'.
  static size_t
  decode(const char* src, size_t src_len, char* dst, size_t dst_len);

  /// Encodes the specified number of characters from the 'data' and writes the
  /// result to the 'output' using URL encoding. The output must have enough
  /// space, e.g. as returned by the calculateEncodedSize().
  static void encodeUrl(const char* data, size_t size, char* output);

  /// Encodes the specified number of characters from the 'data' using URL
  /// encoding.
  static std::string encodeUrl(const char* data, size_t len);

  /// Encodes the specified IOBuf data using URL encoding.
  static std::string encodeUrl(const folly::IOBuf* data);

  /// Encodes the specified text using URL encoding.
  static std::string encodeUrl(folly::StringPiece text);

  /// Decodes the specified URL encoded payload and writes the result to the
  /// 'output'.
  static void decodeUrl(
      const std::pair<const char*, int32_t>& payload,
      std::string& output);

  /// Decodes the specified URL encoded text.
  static std::string decodeUrl(folly::StringPiece text);

  /// Decodes the specified number of characters from the 'src' using URL
  /// encoding and writes the result to the 'dst'.
  static void
  decodeUrl(const char* src, size_t src_len, char* dst, size_t dst_len);

  /// Checks if there is padding in encoded data.
  static inline bool isPadded(const char* data, size_t len) {
    return (len > 0 && data[len - 1] == kPadding) ? true : false;
  }

  /// Counts the number of padding characters in encoded data.
  static inline size_t numPadding(const char* src, size_t len) {
    size_t numPadding{0};
    while (len > 0 && src[len - 1] == kPadding) {
      numPadding++;
      len--;
    }
    return numPadding;
  }

  // Validate the character in charset with ReverseIndex table
  static constexpr bool checkForwardIndex(
      uint8_t idx,
      const Charset& charset,
      const ReverseIndex& reverseIndex) {
  for (uint8_t i = 0; i <= idx; ++i) {
    if (!(reverseIndex[static_cast<uint8_t>(charset[i])] == i)) {
      return false;
    }
  }
  return true;
}

  /// Searches for a character within a charset up to a certain index.
  static const bool findCharacterInCharSet(
      const Charset& charset,
      int base,
      uint8_t idx,
      const char c);

  /// Checks the consistency of a reverse index mapping for a given character
  /// set.
  static constexpr bool checkReverseIndex(
      uint8_t idx,
      const Charset& charset,
      int base,
      const ReverseIndex& reverseIndex)
      {
          for (uint8_t currentIdx = idx; currentIdx != static_cast<uint8_t>(-1);
       --currentIdx) {
    if (reverseIndex[currentIdx] == 255) {
      if (Base64::findCharacterInCharSet(
              charset, base, 0, static_cast<char>(currentIdx))) {
        return false;
      }
    } else {
      if (!(charset[reverseIndex[currentIdx]] == currentIdx)) {
        return false;
      }
    }
  }
  return true;
      }

 private:
  /// Encodes the specified data using the provided charset.
  template <class T>
  static std::string
  encodeImpl(const T& data, const Charset& charset, bool include_pad);

  /// Encodes the specified data using the provided charset.
  template <class T>
  static void encodeImpl(
      const T& data,
      const Charset& charset,
      bool include_pad,
      char* out);

  /// Decodes the specified data using the provided reverse lookup table.
  static size_t decodeImpl(
      const char* src,
      size_t src_len,
      char* dst,
      size_t dst_len,
      const ReverseIndex& table);
};

} // namespace facebook::velox::encoding
