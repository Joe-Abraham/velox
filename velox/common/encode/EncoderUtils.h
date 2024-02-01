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
#include "velox/common/base/Exceptions.h"
namespace facebook::velox::encoding {

constexpr size_t kCharsetSize = 64;
constexpr size_t kReverseIndexSize = 256;

// A character set used for encoding purposes. This array contains the specific
// characters that form the encoding scheme, with each position representing a
// distinct character according to the encoding base. For instance, in base64
// encoding, this array will contain 64 different characters to represent all
// possible base64 values.
using Charset = std::array<char, kCharsetSize>;

// A reverse lookup table for decoding purposes. It maps each possible encoded
// character (represented as an unsigned 8-bit integer index) to its
// corresponding numeric value within the encoding base. Characters that are not
// part of the encoding set are mapped to 255, which indicates an invalid or
// unrecognized character.
using ReverseIndex = std::array<uint8_t, kReverseIndexSize>;

/// Padding character used in encoding, placed at the end of the encoded string
/// when padding is necessary.
constexpr static char kPadding = '=';

// Checks is there padding in encoded data
inline bool isPadded(const char* data, size_t len) {
  return (len > 0 && data[len - 1] == kPadding) ? true : false;
}

// Counts the number of padding characters in encoded data.
inline size_t numPadding(const char* src, size_t len) {
  size_t padding_count = 0;
  for (; len > 0 && src[len - 1] == kPadding; --len) {
    padding_count++;
  }
  return padding_count;
}

/// Retrieves the decoded numeric value corresponding to an encoded character.
///
/// Parameters:
/// - base: The numerical base used for decoding (e.g., base 64).
/// - p: The encoded character to decode, provided as an integer or character.
/// - index: A lookup table that maps encoded characters to their numeric
/// values.
///
/// Returns:
/// - The numeric value associated with the encoded character `p` according to
/// the lookup table.
///
/// Throws:
/// - VELOX_USER_FAIL: If the numeric value of the encoded character is greater
/// than or equal to the specified base, indicating an invalid encoded
/// character.
inline uint8_t baseReverseLookup(int base, char p, const ReverseIndex& reverseIndex) {
  auto curr = reverseIndex[(uint8_t)p];

  if (curr >= base) {
    VELOX_USER_FAIL("decode() - invalid input string: invalid characters");
  }

  return curr;
}

//  Validate the character in charset with ReverseIndex table
constexpr bool checkForwardIndex(
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

/// Checks for the presence of a specified character 'c' within a subset of
/// 'charset'. The search is conducted within the first 'base' characters of
/// 'charset', up to the 'idx' index. Returns true if 'c' is found, false
/// otherwise. This function does not consider the null character '\0' as part
/// of 'charset'.
constexpr bool findCharacterInCharSet(
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

//  Validate the value in ReverseIndex table with charset.
constexpr bool checkReverseIndex(
    uint8_t idx,
    const Charset& charset,
    int base,
    const ReverseIndex& reverseIndex) {
  for (uint8_t currentIdx = idx;; --currentIdx) {
    if (reverseIndex[currentIdx] == 255) {
      if (findCharacterInCharSet(
              charset, base, 0, static_cast<char>(currentIdx))) {
        return false; // Character should not be found
      }
    } else {
      if (!(charset[reverseIndex[currentIdx]] == currentIdx)) {
        return false; // Character at table index does not match currentIdx
      }
    }

    if (currentIdx == 0) {
      break; // Stop the loop when reaching 0 to avoid underflow
    }
  }
  return true;
}

} // namespace facebook::velox::encoding
