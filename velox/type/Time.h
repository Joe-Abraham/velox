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

#include <cstdint>
#include "velox/common/base/Status.h"
#include "velox/type/StringView.h"

namespace facebook::velox::util {

// Constants for time calculations (aligned with TimestampConversion.h)
constexpr const int64_t kMillisInSecond = 1000;
constexpr const int64_t kMillisInMinute = 60 * kMillisInSecond;
constexpr const int64_t kMillisInHour = 60 * kMillisInMinute;
constexpr const int64_t kMillisInDay = 24 * kMillisInHour;

/// Represents parsed time components
struct TimeComponents {
  int32_t hour = 0;
  int32_t minute = 0;
  int32_t second = 0;
  int32_t millis = 0;
};

/// Parse a TIME string (H:m[:s[.SSS]] format)
/// Supports formats:
/// - "H:m" -> "1:30"
/// - "H:m:s" -> "1:30:45"
/// - "H:m:s.SSS" -> "1:30:45.123"
///
/// Returns milliseconds since midnight (0 to 86399999)
/// Returns Unexpected with UserError status if parsing fails
Expected<int64_t> fromTimeString(const char* buf, size_t len);

inline Expected<int64_t> fromTimeString(const StringView& str) {
  return fromTimeString(str.data(), str.size());
}

// Constants for TIME WITH TIME ZONE encoding
constexpr int32_t kMillisShift = 12;
constexpr int32_t kTimezoneMask = (1 << kMillisShift) - 1;
constexpr int16_t kTimeZoneBias = 840;
constexpr int16_t kMinutesInHour = 60;

/// Unpacks the milliseconds since midnight from a packed TIME WITH TIME ZONE
/// value
inline int64_t unpackMillisUtc(int64_t timeWithTimeZone) {
  return timeWithTimeZone >> kMillisShift;
}

/// Unpacks the timezone offset from a packed TIME WITH TIME ZONE value
inline int16_t unpackZoneKeyId(int64_t timeWithTimeZone) {
  return timeWithTimeZone & kTimezoneMask;
}

/// Packs milliseconds since midnight and timezone offset into a 64-bit value
/// The packed value stores time in the upper bits and timezone in the lower 12
/// bits
inline int64_t pack(int64_t millisUtc, int16_t timeZoneKey) {
  return (millisUtc << kMillisShift) | (timeZoneKey & kTimezoneMask);
}

/// Encodes timezone offset using bias encoding to ensure positive values
/// Converts timezone offset from range [-840, 840] to [0, 1680]
inline int16_t biasEncode(int16_t timeZoneOffsetMinutes) {
  VELOX_CHECK(
      -kTimeZoneBias <= timeZoneOffsetMinutes &&
          timeZoneOffsetMinutes <= kTimeZoneBias,
      "Timezone offset must be between -840 and 840 minutes. Got: {}",
      timeZoneOffsetMinutes);
  return timeZoneOffsetMinutes + kTimeZoneBias;
}

/// Parse a TIME WITH TIME ZONE string and return packed 64-bit value
/// Supports formats (with optional space before timezone):
/// - "H:m+HH:mm" or "H:m +HH:mm" -> "1:30+05:30" or "1:30 +05:30"
/// - "H:m:s+HH:mm" or "H:m:s +HH:mm" -> "1:30:45+05:30"
/// - "H:m:s.SSS+HH:mm" -> "1:30:45.123+05:30"
/// - "H:m+HHmm" -> "1:30+0530"
/// - "H:m+HH" or "H:m +HH" -> "1:30+05" or "1:30 +05"
/// - "H:m:s+HH" -> "1:30:45+05"
/// - "H:m:s.SSS+HH" -> "1:30:45.123+05"
///
/// Returns a packed 64-bit value where the upper bits contain milliseconds
/// since midnight and the lower 12 bits contain the bias-encoded timezone
/// offset.
///
/// Returns Unexpected with UserError status if parsing fails
Expected<int64_t> fromTimeWithTimezoneString(const char* buf, size_t len);

inline Expected<int64_t> fromTimeWithTimezoneString(const StringView& str) {
  return fromTimeWithTimezoneString(str.data(), str.size());
}

/// Parse timezone offset from string
/// Supports formats:
/// - "+HH:mm" or "-HH:mm" -> "+05:30", "-08:00"
/// - "+HHmm" or "-HHmm" -> "+0530", "-0800"
/// - "+HH" or "-HH" -> "+05", "-08"
///
/// Returns timezone offset in minutes (-840 to 840)
/// Returns Unexpected with UserError status if parsing fails
Expected<int16_t> parseTimezoneOffset(const char* buf, size_t len);

inline Expected<int16_t> parseTimezoneOffset(const StringView& str) {
  return parseTimezoneOffset(str.data(), str.size());
}

} // namespace facebook::velox::util
