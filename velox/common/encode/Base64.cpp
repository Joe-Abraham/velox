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

#include <folly/Portability.h>
#include <folly/container/Foreach.h>
#include <folly/io/Cursor.h>
#include <stdint.h>

namespace facebook::velox::encoding {

namespace {

class IOBufWrapper {
 private:
  class Iterator {
   public:
    /// Constructs an iterator from the provided IOBuf data.
    /// @param data The IOBuf data to iterate over.
    explicit Iterator(const folly::IOBuf* data) : cs_(data) {}

    /// Advances the iterator to the next position.
    /// @return A reference to the updated iterator.
    Iterator& operator++(int32_t) {
      return *this;
    }

    /// Dereferences the iterator to access the current value.
    /// @return The current value.
    uint8_t operator*() {
      return cs_.read<uint8_t>();
    }

   private:
    folly::io::Cursor cs_;
  };

 public:
  /// Constructs an IOBufWrapper from the provided IOBuf data.
  /// @param data The IOBuf data to wrap.
  explicit IOBufWrapper(const folly::IOBuf* data) : data_(data) {}

  /// Returns the size of the IOBuf chain data.
  /// @return The size of the IOBuf chain data.
  size_t size() const {
    return data_->computeChainDataLength();
  }

  /// Returns an iterator to the beginning of the IOBuf data.
  /// @return An iterator to the beginning of the IOBuf data.
  Iterator begin() const {
    return Iterator(data_);
  }

 private:
  const folly::IOBuf* data_;
};

} // namespace

// Constants defining the size in bytes of binary and encoded blocks for Base64
// encoding.
// Size of a binary block in bytes (3 bytes = 24 bits)
constexpr static int kBinaryBlockByteSize = 3;
// Size of an encoded block in bytes (4 bytes = 24 bits)
constexpr static int kEncodedBlockByteSize = 4;

constexpr static int kBase = 64; // Encoding base

// Character sets for Base64 and Base64 URL encoding
constexpr const Charset kBase64Charset = {
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
    'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};

constexpr const Charset kBase64UrlCharset = {
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
    'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_'};

// Reverse lookup tables for decoding
constexpr const ReverseIndex kBase64ReverseIndexTable = {
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 62,  255,
    255, 255, 63,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  255, 255,
    255, 255, 255, 255, 255, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,
    10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
    25,  255, 255, 255, 255, 255, 255, 26,  27,  28,  29,  30,  31,  32,  33,
    34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,
    49,  50,  51,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255};

constexpr const ReverseIndex kBase64UrlReverseIndexTable = {
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 62,  255,
    62,  255, 63,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  255, 255,
    255, 255, 255, 255, 255, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,
    10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
    25,  255, 255, 255, 255, 63,  255, 26,  27,  28,  29,  30,  31,  32,  33,
    34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,
    49,  50,  51,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255};

// Verify that for every entry in kBase64Charset, the corresponding entry
// in kBase64ReverseIndexTable is correct.
static_assert(
    Base64::checkForwardIndex(
        sizeof(kBase64Charset) - 1,
        kBase64Charset,
        kBase64ReverseIndexTable),
    "kBase64Charset has incorrect entries");

// Verify that for every entry in kBase64UrlCharset, the corresponding entry
// in kBase64UrlReverseIndexTable is correct.
static_assert(
    Base64::checkForwardIndex(
        sizeof(kBase64UrlCharset) - 1,
        kBase64UrlCharset,
        kBase64UrlReverseIndexTable),
    "kBase64UrlCharset has incorrect entries");

// static
const bool Base64::findCharacterInCharSet(
    const Charset& charset,
    int base,
    uint8_t idx,
    const char c) {
  for (; idx < base; ++idx) {
    if (charset[idx] == c) {
      return true;
    }
  }
  return false;
}

// Verify that for every entry in kBase64ReverseIndexTable, the corresponding
// entry in kBase64Charset is correct.
static_assert(
    Base64::checkReverseIndex(
        sizeof(kBase64ReverseIndexTable) - 1,
        kBase64Charset,
        kBase,
        kBase64ReverseIndexTable),
    "kBase64ReverseIndexTable has incorrect entries.");

// Verify that for every entry in kBase64ReverseIndexTable, the corresponding
// entry in kBase64Charset is correct.
static_assert(
    Base64::checkReverseIndex(
        sizeof(kBase64UrlReverseIndexTable) - 1,
        kBase64UrlCharset,
        kBase,
        kBase64UrlReverseIndexTable),
    "kBase64UrlReverseIndexTable has incorrect entries.");

// Implementation of Base64 encoding and decoding functions.
template <class T>
/* static */ std::string
Base64::encodeImpl(const T& data, const Charset& charset, bool include_pad) {
  size_t outlen = calculateEncodedSize(data.size(), include_pad);
  std::string out;
  out.resize(outlen);
  encodeImpl(data, charset, include_pad, out.data());
  return out;
}

// static
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

template <class T>
/* static */ void Base64::encodeImpl(
    const T& data,
    const Charset& charset,
    bool include_pad,
    char* out) {
  auto len = data.size();
  if (len == 0) {
    return;
  }

  auto wp = out;
  auto it = data.begin();

  for (; len > 2; len -= 3) {
    uint32_t curr = uint8_t(*it++) << 16;
    curr |= uint8_t(*it++) << 8;
    curr |= uint8_t(*it++);

    *wp++ = charset[(curr >> 18) & 0x3f];
    *wp++ = charset[(curr >> 12) & 0x3f];
    *wp++ = charset[(curr >> 6) & 0x3f];
    *wp++ = charset[curr & 0x3f];
  }

  if (len > 0) {
    uint32_t curr = uint8_t(*it++) << 16;
    *wp++ = charset[(curr >> 18) & 0x3f];
    if (len > 1) {
      curr |= uint8_t(*it) << 8;
      *wp++ = charset[(curr >> 12) & 0x3f];
      *wp++ = charset[(curr >> 6) & 0x3f];
      if (include_pad) {
        *wp = kPadding;
      }
    } else {
      *wp++ = charset[(curr >> 12) & 0x3f];
      if (include_pad) {
        *wp++ = kPadding;
        *wp = kPadding;
      }
    }
  }
}

// static
void Base64::encode(const char* data, size_t len, char* output) {
  encodeImpl(folly::StringPiece(data, len), kBase64Charset, true, output);
}

// static
std::string Base64::encode(folly::StringPiece text) {
  return encodeImpl(text, kBase64Charset, true);
}

// static
std::string Base64::encode(const char* data, size_t len) {
  return encode(folly::StringPiece(data, len));
}

// static
std::string Base64::encode(const folly::IOBuf* data) {
  return encodeImpl(IOBufWrapper(data), kBase64Charset, true);
}

// static
std::string Base64::decode(folly::StringPiece encoded) {
  std::string output;
  Base64::decode(std::make_pair(encoded.data(), encoded.size()), output);
  return output;
}

// static
void Base64::decode(
    const std::pair<const char*, int32_t>& payload,
    std::string& output) {
  size_t inputSize = payload.second;
  output.resize(calculateDecodedSize(payload.first, inputSize));
  decode(payload.first, inputSize, output.data(), output.size());
}

// static
void Base64::decode(const char* data, size_t size, char* output) {
  size_t out_len = size / 4 * 3;
  Base64::decode(data, size, output, out_len);
}

// static
size_t
Base64::decode(const char* src, size_t src_len, char* dst, size_t dst_len) {
  return decodeImpl(src, src_len, dst, dst_len, kBase64ReverseIndexTable);
}

// static
size_t Base64::calculateDecodedSize(const char* data, size_t& size) {
  if (size == 0) {
    return 0;
  }

  if (isPadded(data, size)) {
    if (size % kEncodedBlockByteSize != 0) {
      VELOX_USER_FAIL(
          "Base64::decode() - invalid input string: "
          "string length is not a multiple of 4.");
    }

    auto needed = (size * kBinaryBlockByteSize) / kEncodedBlockByteSize;
    auto padding = numPadding(data, size);
    size -= padding;

    return needed -
        ((padding * kBinaryBlockByteSize) + (kEncodedBlockByteSize - 1)) /
        kEncodedBlockByteSize;
  }

  auto extra = size % kEncodedBlockByteSize;
  auto needed = (size / kEncodedBlockByteSize) * kBinaryBlockByteSize;

  if (extra) {
    if (extra == 1) {
      VELOX_USER_FAIL(
          "Base64::decode() - invalid input string: "
          "string length cannot be 1 more than a multiple of 4.");
    }
    needed += (extra * kBinaryBlockByteSize) / kEncodedBlockByteSize;
  }

  return needed;
}

// static
size_t Base64::decodeImpl(
    const char* src,
    size_t src_len,
    char* dst,
    size_t dst_len,
    const ReverseIndex& reverse_lookup) {
  if (!src_len) {
    return 0;
  }

  auto needed = calculateDecodedSize(src, src_len);
  if (dst_len < needed) {
    VELOX_USER_FAIL(
        "Base64::decode() - invalid output string: "
        "output string is too small.");
  }

  for (; src_len > 4; src_len -= 4, src += 4, dst += 3) {
    uint32_t last =
        (baseReverseLookup(kBase, src[0], reverse_lookup) << 18) |
        (baseReverseLookup(kBase, src[1], reverse_lookup) << 12) |
        (baseReverseLookup(kBase, src[2], reverse_lookup) << 6) |
        baseReverseLookup(kBase, src[3], reverse_lookup);
    dst[0] = (last >> 16) & 0xff;
    dst[1] = (last >> 8) & 0xff;
    dst[2] = last & 0xff;
  }

  DCHECK(src_len >= 2);
  uint32_t last =
      (baseReverseLookup(kBase, src[0], reverse_lookup) << 18) |
      (baseReverseLookup(kBase, src[1], reverse_lookup) << 12);
  dst[0] = (last >> 16) & 0xff;
  if (src_len > 2) {
    last |= baseReverseLookup(kBase, src[2], reverse_lookup) << 6;
    dst[1] = (last >> 8) & 0xff;
    if (src_len > 3) {
      last |= baseReverseLookup(kBase, src[3], reverse_lookup);
      dst[2] = last & 0xff;
    }
  }

  return needed;
}

// static
std::string Base64::encodeUrl(folly::StringPiece text) {
  return encodeImpl(text, kBase64UrlCharset, false);
}

// static
std::string Base64::encodeUrl(const char* data, size_t len) {
  return encodeUrl(folly::StringPiece(data, len));
}

// static
std::string Base64::encodeUrl(const folly::IOBuf* data) {
  return encodeImpl(IOBufWrapper(data), kBase64UrlCharset, false);
}

// static
void Base64::encodeUrl(const char* data, size_t len, char* output) {
  encodeImpl(folly::StringPiece(data, len), kBase64UrlCharset, true, output);
}

// static
void Base64::decodeUrl(
    const char* src,
    size_t src_len,
    char* dst,
    size_t dst_len) {
  decodeImpl(src, src_len, dst, dst_len, kBase64UrlReverseIndexTable);
}

// static
std::string Base64::decodeUrl(folly::StringPiece encoded) {
  std::string output;
  Base64::decodeUrl(std::make_pair(encoded.data(), encoded.size()), output);
  return output;
}

// static
void Base64::decodeUrl(
    const std::pair<const char*, int32_t>& payload,
    std::string& output) {
  size_t out_len = (payload.second + 3) / 4 * 3;
  output.resize(out_len, '\0');
  out_len = Base64::decodeImpl(
      payload.first,
      payload.second,
      &output[0],
      out_len,
      kBase64UrlReverseIndexTable);
  output.resize(out_len);
}

} // namespace facebook::velox::encoding
