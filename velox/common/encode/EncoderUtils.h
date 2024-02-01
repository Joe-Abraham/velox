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

const size_t kCharsetSize = 64;
const size_t kReverseIndexSize = 256;

/// A character set used for encoding purposes. This array contains the specific
/// characters that form the encoding scheme, with each position representing a
/// distinct character according to the encoding base. For instance, in base64
/// encoding, this array will contain 64 different characters to represent all
/// possible base64 values.
using Charset = std::array<char, kCharsetSize>;

/// A reverse lookup table for decoding purposes. It maps each possible encoded
/// character (represented as an unsigned 8-bit integer index) to its
/// corresponding numeric value within the encoding base. Characters that are
/// not part of the encoding set are mapped to 255, which indicates an invalid
/// or unrecognized character.
using ReverseIndex = std::array<uint8_t, kReverseIndexSize>;

/// Padding character used in encoding, placed at the end of the encoded string
/// when padding is necessary.
const static char kPadding = '=';

// Checks is there padding in encoded data
inline bool isPadded(const char* data, size_t len) {
  return (len > 0 && data[len - 1] == kPadding) ? true : false;
}

// Counts the number of padding characters in encoded data.
inline size_t numPadding(const char* src, size_t len) {
     size_t numPadding{0};
     while (len > 0 && src[len - 1] == kPadding) {
       numPadding++;
       len--;
     }

     return numPadding;
   }

/// Performs a reverse lookup in the reverse index to retrieve the original
/// index of a character in the base.
/// @param base The base of the encoding.
/// @param p The character for which the reverse lookup is performed.
/// @param reverseIndex The reverse index mapping characters to their original
/// indices.
/// @return The original index of the character within the base.
inline uint8_t
baseReverseLookup(int base, char p, const ReverseIndex& reverseIndex) {
  auto curr = reverseIndex[(uint8_t)p];

  if (curr >= base) {
    VELOX_USER_FAIL("decode() - invalid input string: invalid characters");
  }

  return curr;
}

//  Validate the character in charset with ReverseIndex table
const bool checkForwardIndex(
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
/// @param charset The character set to search within.
/// @param base The upper limit index for searching.
/// @param idx The starting index for the search.
/// @param c The character to find within the charset.
/// @return True if the character is found within the charset up to the
/// specified index, otherwise false.
const bool findCharacterInCharSet(
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

/// Checks the consistency of a reverse index mapping for a given character set.
///
/// @param idx Starting index for the reverse index check.
/// @param charset The character set being checked.
/// @param base The base index used for character lookups in the character set.
/// @param reverseIndex The reverse index mapping to validate.
/// @return `true` if all character mappings are consistent, `false` otherwise.
///
/// The function iterates backward from the given `idx`, validating each index
/// in `reverseIndex`:
/// - If the reverse index entry is `255`, this indicates an unused or unmapped
///   entry. The function checks that no character corresponding to the current
///   index is found in the given charset.
/// - If the reverse index entry is not `255`, it should point to a valid index
///   in the `charset`. The function
///   ensures that the character at that index matches the current index.
/// - The loop terminates at index 0 to avoid underflow.

const bool checkReverseIndex(
    uint8_t idx,
    const Charset& charset,
    int base,
    const ReverseIndex& reverseIndex) {
  for (uint8_t currentIdx = idx; currentIdx != static_cast<uint8_t>(-1);
       --currentIdx) {
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
  }
  return true;
}

} // namespace facebook::velox::encoding
