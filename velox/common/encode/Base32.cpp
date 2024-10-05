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
#include "velox/common/encode/Base32.h"

#include <glog/logging.h>

namespace facebook::velox::encoding {

// Constants defining the size in bytes of binary and encoded blocks for Base32
// encoding.
// Size of a binary block in bytes (5 bytes = 40 bits)
constexpr static int kBinaryBlockByteSize = 5;
// Size of an encoded block in bytes (8 bytes = 40 bits)
constexpr static int kEncodedBlockByteSize = 8;

constexpr Base32::Charset kBase32Charset = {
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
    'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
    'W', 'X', 'Y', 'Z', '2', '3', '4', '5', '6', '7'};

constexpr Base32::ReverseIndex kBase32ReverseIndexTable = {
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 26,  27,  28,  29,  30,  31,  255, 255, 255, 255,
    255, 255, 255, 255, 255, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,
    10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
    25,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255};

// Verify that for each 32 entries in kBase32Charset, the corresponding entry
// in kBase32ReverseIndexTable is correct.
static_assert(
    checkForwardIndex(
        sizeof(kBase32Charset) / 2 - 1,
        kBase32Charset,
        kBase32ReverseIndexTable),
    "kBase32Charset has incorrect entries");

// Verify that for every entry in kBase32ReverseIndexTable, the corresponding
// entry in kBase32Charset is correct.
static_assert(
    checkReverseIndex(
        sizeof(kBase32ReverseIndexTable) - 1,
        kBase32Charset,
        kBase32ReverseIndexTable),
    "kBase32ReverseIndexTable has incorrect entries.");

// static
uint8_t Base32::base32ReverseLookup(
    char p,
    const Base32::ReverseIndex& reverseIndex,
    Status& status) {
  return reverseLookup(p, reverseIndex, status, Base32::kCharsetSize);
}

// static
Status Base32::decode(std::string_view input, std::string& output) {
  return decodeImpl(
      input,
      input.size(),
      output.data(),
      output.size(),
      kBase32ReverseIndexTable);
}

// static
Status Base32::decodeImpl(
    std::string_view input,
    size_t inputSize,
    char* output,
    size_t outputSize,
    const Base32::ReverseIndex& reverseIndex) {
  // Check if input is empty
  if (input.empty()) {
    return Status::OK();
  }

  size_t decodedSize;
  // Calculate decoded size and check for status
  auto status = calculateDecodedSize(
      input,
      inputSize,
      decodedSize,
      kBinaryBlockByteSize,
      kEncodedBlockByteSize);
  if (!status.ok()) {
    return status;
  }

  if (outputSize < decodedSize) {
    return Status::UserError("Base32::decode() - output buffer too small.");
  }

  Status lookupStatus;
  // Handle full groups of 8 characters.
  while (inputSize >= 8) {
    // Each character of the 8 bytes encodes 5 bits of the original, grab each
    // with the appropriate shifts to rebuild the original and then split that
    // back into the original 8-bit bytes.
    uint64_t last =
        (uint64_t(base32ReverseLookup(input[0], reverseIndex, lookupStatus))
         << 35) |
        (uint64_t(base32ReverseLookup(input[1], reverseIndex, lookupStatus))
         << 30) |
        (base32ReverseLookup(input[2], reverseIndex, lookupStatus) << 25) |
        (base32ReverseLookup(input[3], reverseIndex, lookupStatus) << 20) |
        (base32ReverseLookup(input[4], reverseIndex, lookupStatus) << 15) |
        (base32ReverseLookup(input[5], reverseIndex, lookupStatus) << 10) |
        (base32ReverseLookup(input[6], reverseIndex, lookupStatus) << 5) |
        base32ReverseLookup(input[7], reverseIndex, lookupStatus);

    output[0] = (last >> 32) & 0xff;
    output[1] = (last >> 24) & 0xff;
    output[2] = (last >> 16) & 0xff;
    output[3] = (last >> 8) & 0xff;
    output[4] = last & 0xff;

    // Move the input string_view forward
    input.remove_prefix(8);
    output += 5;
    inputSize -= 8;
  }

  // Handle the last 2, 4, 5, 7, or 8 characters.
  if (inputSize >= 2) {
    uint64_t last =
        (uint64_t(base32ReverseLookup(input[0], reverseIndex, lookupStatus))
         << 35) |
        (uint64_t(base32ReverseLookup(input[1], reverseIndex, lookupStatus))
         << 30);
    output[0] = (last >> 32) & 0xff;

    if (inputSize > 2) {
      last |= base32ReverseLookup(input[2], reverseIndex, lookupStatus) << 25;
      last |= base32ReverseLookup(input[3], reverseIndex, lookupStatus) << 20;
      output[1] = (last >> 24) & 0xff;

      if (inputSize > 4) {
        last |= base32ReverseLookup(input[4], reverseIndex, lookupStatus) << 15;
        output[2] = (last >> 16) & 0xff;

        if (inputSize > 5) {
          last |= base32ReverseLookup(input[5], reverseIndex, lookupStatus)
              << 10;
          last |= base32ReverseLookup(input[6], reverseIndex, lookupStatus)
              << 5;
          output[3] = (last >> 8) & 0xff;

          if (inputSize > 7) {
            last |= base32ReverseLookup(input[7], reverseIndex, lookupStatus);
            output[4] = last & 0xff;
          }
        }
      }
    }
  }

  return lookupStatus.ok() ? Status::OK() : lookupStatus;
}

} // namespace facebook::velox::encoding
                                        