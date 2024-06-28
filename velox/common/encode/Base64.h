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
class Base64 {
 public:
  /// Padding character used in encoding.
  const static char kPadding = '=';

  /// Returns encoded size for the input of the specified size.
  /// @param size The size of the input data.
  /// @param withPadding Whether padding is included.
  /// @return The encoded size.
  static size_t calculateEncodedSize(size_t size, bool withPadding = true);

  /// Encodes the specified number of characters from the 'data' and writes the
  /// result to the 'output'. The output must have enough space, e.g. as
  /// returned by the calculateEncodedSize().
  /// @param data The input data to encode.
  /// @param size The size of the input data.
  /// @param output The output buffer to store the encoded data.
  static void encode(const char* data, size_t size, char* output);

  /// Encodes the specified number of characters from the 'data'.
  /// @param data The input data to encode.
  /// @param len The length of the input data.
  /// @return The encoded string.
  static std::string encode(const char* data, size_t len);

  /// Encodes the specified text.
  /// @param text The input text to encode.
  /// @return The encoded string.
  static std::string encode(folly::StringPiece text);

  /// Encodes the specified IOBuf data.
  /// @param text The input IOBuf data to encode.
  /// @return The encoded string.
  static std::string encode(const folly::IOBuf* text);

  /// Returns the actual size of the decoded data. Will also remove the padding
  /// length from the input data 'size'.
  /// @param data The input encoded data.
  /// @param size The size of the input data, updated to remove padding.
  /// @return The decoded size.
  static size_t calculateDecodedSize(const char* data, size_t& size);

  /// Decodes the specified number of characters from the 'data' and writes the
  /// result to the 'output'. The output must have enough space, e.g. as
  /// returned by the calculateDecodedSize().
  /// @param data The input encoded data.
  /// @param size The size of the input data.
  /// @param output The output buffer to store the decoded data.
  static void decode(const char* data, size_t size, char* output);

  /// Decodes the specified payload and writes the result to the 'output'.
  /// @param payload The input encoded payload.
  /// @param output The output string to store the decoded data.
  static void decode(
      const std::pair<const char*, int32_t>& payload,
      std::string& output);

  /// Decodes the specified encoded text.
  /// @param encoded The input encoded text.
  /// @return The decoded string.
  static std::string decode(folly::StringPiece encoded);

  /// Decodes the specified number of characters from the 'src' and writes the
  /// result to the 'dst'.
  /// @param src The input encoded data.
  /// @param src_len The length of the input data.
  /// @param dst The output buffer to store the decoded data.
  /// @param dst_len The length of the output buffer.
  /// @return The number of decoded bytes.
  static size_t
  decode(const char* src, size_t src_len, char* dst, size_t dst_len);

  /// Encodes the specified number of characters from the 'data' and writes the
  /// result to the 'output' using URL encoding. The output must have enough
  /// space, e.g. as returned by the calculateEncodedSize().
  /// @param data The input data to encode.
  /// @param size The size of the input data.
  /// @param output The output buffer to store the encoded data.
  static void encodeUrl(const char* data, size_t size, char* output);

  /// Encodes the specified number of characters from the 'data' using URL
  /// encoding.
  /// @param data The input data to encode.
  /// @param len The length of the input data.
  /// @return The encoded string.
  static std::string encodeUrl(const char* data, size_t len);

  /// Encodes the specified IOBuf data using URL encoding.
  /// @param data The input IOBuf data to encode.
  /// @return The encoded string.
  static std::string encodeUrl(const folly::IOBuf* data);

  /// Encodes the specified text using URL encoding.
  /// @param text The input text to encode.
  /// @return The encoded string.
  static std::string encodeUrl(folly::StringPiece text);

  /// Decodes the specified URL encoded payload and writes the result to the
  /// 'output'.
  /// @param payload The input encoded payload.
  /// @param output The output string to store the decoded data.
  static void decodeUrl(
      const std::pair<const char*, int32_t>& payload,
      std::string& output);

  /// Decodes the specified URL encoded text.
  /// @param text The input encoded text.
  /// @return The decoded string.
  static std::string decodeUrl(folly::StringPiece text);

  /// Decodes the specified number of characters from the 'src' using URL
  /// encoding and writes the result to the 'dst'.
  /// @param src The input encoded data.
  /// @param src_len The length of the input data.
  /// @param dst The output buffer to store the decoded data.
  /// @param dst_len The length of the output buffer.
  static void
  decodeUrl(const char* src, size_t src_len, char* dst, size_t dst_len);

 private:
  /// Checks if there is padding in encoded data.
  /// @param data The data to check for padding.
  /// @param len The length of the data.
  /// @return True if there is padding, otherwise false.
  static inline bool isPadded(const char* data, size_t len) {
    return (len > 0 && data[len - 1] == kPadding) ? true : false;
  }

  /// Counts the number of padding characters in encoded data.
  /// @param src The source data to check for padding.
  /// @param len The length of the source data.
  /// @return The number of padding characters.
  static inline size_t numPadding(const char* src, size_t len) {
    size_t numPadding{0};
    while (len > 0 && src[len - 1] == kPadding) {
      numPadding++;
      len--;
    }
    return numPadding;
  }
  /// Encodes the specified data using the provided charset.
  /// @tparam T The type of the input data.
  /// @param data The input data to encode.
  /// @param charset The charset used for encoding.
  /// @param include_pad Whether to include padding.
  /// @return The encoded string.
  template <class T>
  static std::string
  encodeImpl(const T& data, const Charset& charset, bool include_pad);

  /// Encodes the specified data using the provided charset.
  /// @tparam T The type of the input data.
  /// @param data The input data to encode.
  /// @param charset The charset used for encoding.
  /// @param include_pad Whether to include padding.
  /// @param out The output buffer to store the encoded data.
  template <class T>
  static void encodeImpl(
      const T& data,
      const Charset& charset,
      bool include_pad,
      char* out);

  /// Decodes the specified data using the provided reverse lookup table.
  /// @param src The input encoded data.
  /// @param src_len The length of the input data.
  /// @param dst The output buffer to store the decoded data.
  /// @param dst_len The length of the output buffer.
  /// @param table The reverse lookup table used for decoding.
  /// @return The number of decoded bytes.
  static size_t decodeImpl(
      const char* src,
      size_t src_len,
      char* dst,
      size_t dst_len,
      const ReverseIndex& table);
};

} // namespace facebook::velox::encoding
