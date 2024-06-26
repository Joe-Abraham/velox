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
#include "velox/common/encode/Base64.h"

namespace facebook::velox::encoding {

const std::string Base64::base64_chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789+/";

const std::string Base64::base64_url_chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789-_";

inline bool Base64::is_base64(unsigned char c) {
  return (isalnum(c) || (c == '+') || (c == '/') || (c == '-') || (c == '_'));
}

std::string Base64::encode(const char* data, size_t len) {
  std::string output;
  encode_impl(data, len, output, base64_chars);
  return output;
}

std::string Base64::encode(folly::StringPiece text) {
  return encode(text.data(), text.size());
}

std::string Base64::encode(const folly::IOBuf* text) {
  folly::io::Cursor cursor(text);
  std::string data(text->computeChainDataLength(), '\0');
  cursor.pull((uint8_t*)data.data(), data.size());
  return encode(data.data(), data.size());
}

void Base64::encode_impl(
    const char* data,
    size_t len,
    std::string& output,
    const std::string& chars) {
  unsigned char const* bytes_to_encode =
      reinterpret_cast<const unsigned char*>(data);
  int i = 0;
  int j = 0;
  unsigned char char_array_3[3];
  unsigned char char_array_4[4];

  while (len--) {
    char_array_3[i++] = *(bytes_to_encode++);
    if (i == 3) {
      char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
      char_array_4[1] =
          ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
      char_array_4[2] =
          ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
      char_array_4[3] = char_array_3[2] & 0x3f;

      for (i = 0; (i < 4); i++)
        output += chars[char_array_4[i]];
      i = 0;
    }
  }

  if (i) {
    for (j = i; j < 3; j++)
      char_array_3[j] = '\0';

    char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
    char_array_4[1] =
        ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
    char_array_4[2] =
        ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
    char_array_4[3] = char_array_3[2] & 0x3f;

    for (j = 0; (j < i + 1); j++)
      output += chars[char_array_4[j]];

    while ((i++ < 3))
      output += '=';
  }
}

std::string Base64::decode(folly::StringPiece encoded) {
  std::string output;
  decode_impl(encoded.data(), encoded.size(), output, base64_chars);
  return output;
}

void Base64::decode(const char* data, size_t size, std::string& output) {
  decode_impl(data, size, output, base64_chars);
}

void Base64::decode_impl(
    const char* data,
    size_t size,
    std::string& output,
    const std::string& chars) {
  int in_len = size;
  int i = 0;
  int j = 0;
  int in_ = 0;
  unsigned char char_array_4[4], char_array_3[3];

  while (in_len-- && (data[in_] != '=') && is_base64(data[in_])) {
    char_array_4[i++] = data[in_];
    in_++;
    if (i == 4) {
      for (i = 0; i < 4; i++)
        char_array_4[i] = chars.find(char_array_4[i]);

      char_array_3[0] =
          (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
      char_array_3[1] =
          ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
      char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

      for (i = 0; (i < 3); i++)
        output += char_array_3[i];
      i = 0;
    }
  }

  if (i) {
    for (j = i; j < 4; j++)
      char_array_4[j] = 0;

    for (j = 0; j < 4; j++)
      char_array_4[j] = chars.find(char_array_4[j]);

    char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
    char_array_3[1] =
        ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
    char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

    for (j = 0; (j < i - 1); j++)
      output += char_array_3[j];
  }
}

std::string Base64::encodeUrl(const char* data, size_t len) {
  std::string output;
  encode_impl(data, len, output, base64_url_chars);
  return output;
}

std::string Base64::encodeUrl(folly::StringPiece text) {
  return encodeUrl(text.data(), text.size());
}

std::string Base64::encodeUrl(const folly::IOBuf* data) {
  folly::io::Cursor cursor(data);
  std::string str(data->computeChainDataLength(), '\0');
  cursor.pull((uint8_t*)str.data(), str.size());
  return encodeUrl(str.data(), str.size());
}

std::string Base64::decodeUrl(folly::StringPiece encoded) {
  std::string output;
  decode_impl(encoded.data(), encoded.size(), output, base64_url_chars);
  return output;
}

void Base64::decodeUrl(const char* data, size_t size, std::string& output) {
  decode_impl(data, size, output, base64_url_chars);
}

size_t Base64::calculateEncodedSize(size_t size, bool withPadding) {
  if (size == 0) {
    return 0;
  }
  size_t encodedSize = ((size + 2) / 3) * 4;
  if (!withPadding) {
    encodedSize -= (3 - (size % 3)) % 3;
  }
  return encodedSize;
}

size_t Base64::calculateDecodedSize(const char* data, size_t size, bool isUrl) {
  if (size == 0) {
    return 0;
  }
  size_t padding = 0;
  if (!isUrl) {
    if (data[size - 1] == '=') {
      padding++;
      if (data[size - 2] == '=') {
        padding++;
      }
    }
  }
  return (size / 4) * 3 - padding;
}

} // namespace facebook::velox::encoding
