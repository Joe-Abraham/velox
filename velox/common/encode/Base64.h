/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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

#include <folly/Range.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <exception>
#include <string>
#include <vector>

namespace facebook::velox::encoding {

class Base64Exception : public std::exception {
 public:
  explicit Base64Exception(const char* msg) : msg_(msg) {}
  const char* what() const noexcept override {
    return msg_;
  }

 protected:
  const char* msg_;
};

class Base64 {
 public:
  static std::string encode(const char* data, size_t len);
  static std::string encode(folly::StringPiece text);
  static std::string encode(const folly::IOBuf* text);

  static std::string decode(folly::StringPiece encoded);
  static void decode(const char* data, size_t size, std::string& output);

  static std::string encodeUrl(const char* data, size_t len);
  static std::string encodeUrl(folly::StringPiece text);
  static std::string encodeUrl(const folly::IOBuf* data);

  static std::string decodeUrl(folly::StringPiece encoded);
  static void decodeUrl(const char* data, size_t size, std::string& output);

  static size_t calculateEncodedSize(size_t size, bool withPadding = true);
  static size_t
  calculateDecodedSize(const char* data, size_t size, bool isUrl = false);

 private:
  static const std::string base64_chars;
  static const std::string base64_url_chars;

  static inline bool is_base64(unsigned char c);
  static void encode_impl(
      const char* data,
      size_t len,
      std::string& output,
      const std::string& chars);
  static void decode_impl(
      const char* data,
      size_t size,
      std::string& output,
      const std::string& chars);
};

} // namespace facebook::velox::encoding
