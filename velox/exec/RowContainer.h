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

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/ContainerRowSerde.h"
#include "velox/exec/Spill.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox::exec {
namespace test {
class RowContainerTestHelper;
}

class Aggregate;

class Accumulator {
 public:
  Accumulator(
      bool isFixedSize,
      int32_t fixedSize,
      bool usesExternalMemory,
      int32_t alignment,
      TypePtr spillType,
      std::function<void(folly::Range<char**> groups, VectorPtr& result)>
          spillExtractFunction,
      std::function<void(folly::Range<char**> groups)> destroyFunction);

  explicit Accumulator(Aggregate* aggregate, TypePtr spillType);

  bool isFixedSize() const;

  int32_t fixedWidthSize() const;

  bool usesExternalMemory() const;

  int32_t alignment() const;

  const TypePtr& spillType() const;

  void extractForSpill(folly::Range<char**> groups, VectorPtr& result) const;

  void destroy(folly::Range<char**> groups);

 private:
  const bool isFixedSize_;
  const int32_t fixedSize_;
  const bool usesExternalMemory_;
  const int32_t alignment_;
  const TypePtr spillType_;
  std::function<void(folly::Range<char**>, VectorPtr&)> spillExtractFunction_;
  std::function<void(folly::Range<char**> groups)> destroyFunction_;
};

using normalized_key_t = uint64_t;

struct RowContainerIterator {
  int32_t allocationIndex = 0;
  int32_t rowOffset = 0;
  /// Number of unvisited entries that are prefixed by an uint64_t for
  /// normalized key. Set in listRows() on first call.
  int64_t normalizedKeysLeft = 0;
  int normalizedKeySize = 0;

  /// Ordinal position of 'currentRow' in RowContainer.
  int32_t rowNumber{0};
  char* rowBegin{nullptr};
  /// First byte after the end of the range containing 'currentRow'.
  char* endOfRun{nullptr};

  /// Returns the current row, skipping a possible normalized key below the
  /// first byte of row.
  inline char* currentRow() const {
    return (rowBegin && normalizedKeysLeft) ? rowBegin + normalizedKeySize
                                            : rowBegin;
  }

  void reset() {
    *this = {};
  }

  std::string toString() const;
};

/// Container with a 8-bit partition number field for each row in a
/// RowContainer. The partition number bytes correspond 1:1 to rows. Used only
/// for parallel hash join build.
class RowPartitions {
 public:
  /// Initializes this to hold up to 'numRows'.
  RowPartitions(int32_t numRows, memory::MemoryPool& pool);

  /// Appends 'partitions' to the end of 'this'. Throws if adding more than the
  /// capacity given at construction.
  void appendPartitions(folly::Range<const uint8_t*> partitions);

  auto& allocation() const {
    return allocation_;
  }

  int32_t size() const {
    return size_;
  }

 private:
  const int32_t capacity_;

  // Number of partition numbers added.
  int32_t size_{0};

  // Partition numbers. 1 byte each.
  memory::Allocation allocation_;
};

/// Packed representation of offset, null byte offset and null mask for
/// a column inside a RowContainer.
class RowColumn {
 public:
  /// Used as null offset for a non-null column.
  static constexpr int32_t kNotNullOffset = -1;

  RowColumn(int32_t offset, int32_t nullOffset)
      : packedOffsets_(PackOffsets(offset, nullOffset)) {}

  int32_t offset() const {
    return packedOffsets_ >> 32;
  }

  int32_t nullByte() const {
    return static_cast<uint32_t>(packedOffsets_) >> 8;
  }

  uint8_t nullMask() const {
    return packedOffsets_ & 0xff;
  }

  /// The null bits and the initialized bits for accumulators start at the
  /// beginning of the first byte following the null bits for the keys.  This
  /// guarantees that they always appear on the same byte for any given
  /// accumulator (since 2 evenly divides 8).
  int32_t initializedByte() const {
    return nullByte();
  }

  /// The initialized bit for an accumulator is guaranteed to appear on the same
  /// byte immediately following the null bit for that accumulator.
  int32_t initializedMask() const {
    return nullMask() << 1;
  }

  /// Aggregated stats of a column in the 'RowContainer'.
  class Stats {
   public:
    Stats() = default;

    void addCellSize(int32_t bytes) {
      if (UNLIKELY(nonNullCount_ == 0)) {
        minBytes_ = bytes;
        maxBytes_ = bytes;
      } else {
        minBytes_ = std::min(minBytes_, bytes);
        maxBytes_ = std::max(maxBytes_, bytes);
      }
      sumBytes_ += bytes;
      ++nonNullCount_;
    }

    void addNullCell() {
      ++nullCount_;
    }

    void removeOrUpdateCellStats(int32_t bytes, bool wasNull, bool setToNull);

    int32_t maxBytes() const {
      return maxBytes_;
    }

    int32_t minBytes() const {
      return minBytes_;
    }

    uint64_t sumBytes() const {
      return sumBytes_;
    }

    uint64_t avgBytes() const {
      if (nonNullCount_ == 0) {
        return 0;
      }
      return sumBytes_ / nonNullCount_;
    }

    uint32_t nonNullCount() const {
      return nonNullCount_;
    }

    uint32_t nullCount() const {
      return nullCount_;
    }

    uint32_t numCells() const {
      return nullCount_ + nonNullCount_;
    }

    void invalidateMinMaxColumnStats() {
      minMaxStatsValid_ = false;
    }

    bool minMaxColumnStatsValid() const {
      return minMaxStatsValid_;
    }

    /// Merges multiple aggregated stats of the same column into a single one.
    static Stats merge(const std::vector<Stats>& statsList);

   private:
    // Aggregated stats for non-null rows of the column.
    int32_t minBytes_{0};
    int32_t maxBytes_{0};
    bool minMaxStatsValid_{true};
    uint64_t sumBytes_{0};

    uint32_t nonNullCount_{0};
    uint32_t nullCount_{0};
  };

 private:
  static uint64_t PackOffsets(int32_t offset, int32_t nullOffset) {
    if (nullOffset == kNotNullOffset) {
      // If the column is not nullable, The low word is 0, meaning
      // that a null check will AND 0 to the 0th byte of the row,
      // which is always false and always safe to do.
      return static_cast<uint64_t>(offset) << 32;
    }
    return (1UL << (nullOffset & 7)) | ((nullOffset & ~7UL) << 5) |
        static_cast<uint64_t>(offset) << 32;
  }

  const uint64_t packedOffsets_;
};

/// Collection of rows for aggregation, hash join, order by.
class RowContainer {
 public:
  static constexpr uint64_t kUnlimited = std::numeric_limits<uint64_t>::max();
  /// The number of flags (bits) per accumulator, one for null and one for
  /// initialized.
  static constexpr size_t kNumAccumulatorFlags = 2;
  using Eraser = std::function<void(folly::Range<char**> rows)>;

  /// 'keyTypes' gives the type of row and use 'allocator' for bulk
  /// allocation.
  RowContainer(const std::vector<TypePtr>& keyTypes, memory::MemoryPool* pool)
      : RowContainer(keyTypes, std::vector<TypePtr>{}, pool) {}

  RowContainer(
      const std::vector<TypePtr>& keyTypes,
      const std::vector<TypePtr>& dependentTypes,
      memory::MemoryPool* pool)
      : RowContainer(
            keyTypes,
            true, // nullableKeys
            std::vector<Accumulator>{},
            dependentTypes,
            false, // hasNext
            false, // isJoinBuild
            false, // hasProbedFlag
            false, // hasNormalizedKey
            pool) {}

  ~RowContainer();

  static int32_t combineAlignments(int32_t a, int32_t b);

  /// 'keyTypes' gives the type of the key of each row. For a group by,
  /// order by or right outer join build side these may be
  /// nullable. 'nullableKeys' specifies if these have a null flag.
  /// 'aggregates' is a vector of Aggregate for a group by payload,
  /// empty otherwise. 'DependentTypes' gives the types of non-key
  /// columns for a hash join build side or an order by. 'hasNext' is
  /// true for a hash join build side where keys can be
  /// non-unique. 'isJoinBuild' is true for hash join build sides. This
  /// implies that hashing of keys ignores null keys even if these were
  /// allowed. 'hasProbedFlag' indicates that an extra bit is reserved
  /// for a probed state of a full or right outer
  /// join. 'hasNormalizedKey' specifies that an extra word is left
  /// below each row for a normalized key that collapses all parts
  /// into one word for faster comparison. The bulk allocation is done
  /// from 'allocator'. ContainerRowSerde is used for serializing complex
  /// type values into the container.
  RowContainer(
      const std::vector<TypePtr>& keyTypes,
      bool nullableKeys,
      const std::vector<Accumulator>& accumulators,
      const std::vector<TypePtr>& dependentTypes,
      bool hasNext,
      bool isJoinBuild,
      bool hasProbedFlag,
      bool hasNormalizedKey,
      memory::MemoryPool* pool);

  /// Allocates a new row and initializes possible aggregates to null.
  char* newRow();

  uint32_t rowSize(const char* row) const {
    return fixedRowSize_ +
        (rowSizeOffset_
             ? *reinterpret_cast<const uint32_t*>(row + rowSizeOffset_)
             : 0);
  }

  /// Sets all fields, aggregates, keys and dependents to null. Used when making
  /// a row with uninitialized keys for aggregates with no-op partial
  /// aggregation.
  void setAllNull(char* row);

  /// The row size excluding any out-of-line stored variable length values.
  int32_t fixedRowSize() const {
    return fixedRowSize_;
  }

  /// Adds 'rows' to the free rows list and frees any associated variable length
  /// data.
  void eraseRows(folly::Range<char**> rows);

  /// Copies elements of 'rows' where the char* points to a row inside 'this' to
  /// 'result' and returns the number copied. 'result' should have space for
  /// 'rows.size()'.
  int32_t findRows(folly::Range<char**> rows, char** result) const;

  void incrementRowSize(char* row, uint64_t bytes) {
    uint32_t* ptr = reinterpret_cast<uint32_t*>(row + rowSizeOffset_);
    uint64_t size = *ptr + bytes;
    *ptr = std::min<uint64_t>(size, std::numeric_limits<uint32_t>::max());
  }

  /// Initialize row. 'reuse' specifies whether the 'row' is reused or not. If
  /// it is reused, it will free memory associated with the row elsewhere (such
  /// as in HashStringAllocator).
  /// Note: Fields of the row are not zero-initialized. If the row contains
  /// variable-width fields, the caller must populate these fields by calling
  /// 'store' or initialize them to zero by calling 'initializeFields'.
  char* initializeRow(char* row, bool reuse);

  /// Zero out all the fields of the 'row'.
  void initializeFields(char* row) {
    ::memset(row, 0, fixedRowSize_);
  }

  /// Stores the 'index'th value in 'decoded' into 'row' at 'columnIndex'.
  void store(
      const DecodedVector& decoded,
      vector_size_t rowIndex,
      char* row,
      int32_t columnIndex);

  /// Stores the first 'rows.size' values from the 'decoded' vector into the
  /// 'columnIndex' column of 'rows'.
  void store(
      const DecodedVector& decoded,
      folly::Range<char**> rows,
      int32_t columnIndex);

  HashStringAllocator& stringAllocator() {
    return *stringAllocator_;
  }

  /// Returns the number of used rows in 'this'. This is the number of rows a
  /// RowContainerIterator would access.
  int64_t numRows() const {
    return numRows_;
  }

  /// Copy key and dependent columns into a flat VARBINARY vector. All columns
  /// of a row are copied into a single buffer. The format of that buffer is an
  /// implementation detail. The data can be loaded back into the RowContainer
  /// using 'storeSerializedRow'.
  ///
  /// Used for spilling as it is more efficient than converting from row to
  /// columnar format.
  void extractSerializedRows(folly::Range<char**> rows, const VectorPtr& result)
      const;

  /// Copies serialized row produced by 'extractSerializedRow' into the
  /// container.
  void storeSerializedRow(
      const FlatVector<StringView>& vector,
      vector_size_t index,
      char* row);

  /// Copies the values at 'col' into 'result' (starting at 'resultOffset')
  /// for the 'numRows' rows pointed to by 'rows'. If a 'row' is null, sets
  /// corresponding row in 'result' to null.
  /// @param columnHasNulls indicates whether the 'col' column contains null
  /// values. If 'columnHasNulls' is false, a null-free optimization will be
  /// applied. It is the caller's responsibility to ensure this flag is set
  /// correctly.
  static void extractColumn(
      const char* const* rows,
      int32_t numRows,
      RowColumn col,
      bool columnHasNulls,
      vector_size_t resultOffset,
      const VectorPtr& result);

  /// Copies the values at 'col' into 'result' for the 'numRows' rows pointed to
  /// by 'rows'. If an entry in 'rows' is null, sets corresponding row in
  /// 'result' to null.
  /// @param columnHasNulls indicates whether the 'col' column contains null
  /// values. If 'columnHasNulls' is false, a null-free optimization will be
  /// applied. It is the caller's responsibility to ensure this flag is set
  /// correctly.
  static void extractColumn(
      const char* const* rows,
      int32_t numRows,
      RowColumn col,
      bool columnHasNulls,
      const VectorPtr& result) {
    extractColumn(rows, numRows, col, columnHasNulls, 0, result);
  }

  /// Copies the values from the array pointed to by 'rows' at 'col' into
  /// 'result' (starting at 'resultOffset') for the rows at positions in
  /// the 'rowNumbers' array. If a 'row' is null, sets corresponding row in
  /// 'result' to null. The positions in 'rowNumbers' array can repeat and also
  /// appear out of order. If rowNumbers has a negative value, then the
  /// corresponding row in 'result' is set to null.
  /// @param columnHasNulls indicates whether the 'col' column contains null
  /// values. If 'columnHasNulls' is false, a null-free optimization will be
  /// applied. It is the caller's responsibility to ensure this flag is set
  /// correctly.
  static void extractColumn(
      const char* const* rows,
      folly::Range<const vector_size_t*> rowNumbers,
      RowColumn col,
      bool columnHasNulls,
      vector_size_t resultOffset,
      const VectorPtr& result);

  /// Sets in result all locations with null values in col for rows (for numRows
  /// number of rows).
  static void extractNulls(
      const char* const* rows,
      int32_t numRows,
      RowColumn col,
      const BufferPtr& result);

  /// Copies the values at 'columnIndex' into 'result' for the 'numRows' rows
  /// pointed to by 'rows'. If an entry in 'rows' is null, sets corresponding
  /// row in 'result' to null.
  void extractColumn(
      const char* const* rows,
      int32_t numRows,
      int32_t columnIndex,
      const VectorPtr& result) const {
    extractColumn(
        rows,
        numRows,
        columnAt(columnIndex),
        columnHasNulls(columnIndex),
        result);
  }

  /// Copies the values at 'columnIndex' into 'result' (starting at
  /// 'resultOffset') for the 'numRows' rows pointed to by 'rows'. If an
  /// entry in 'rows' is null, sets corresponding row in 'result' to null.
  void extractColumn(
      const char* const* rows,
      int32_t numRows,
      int32_t columnIndex,
      int32_t resultOffset,
      const VectorPtr& result) const {
    extractColumn(
        rows,
        numRows,
        columnAt(columnIndex),
        columnHasNulls(columnIndex),
        resultOffset,
        result);
  }

  /// Copies the values at 'columnIndex' at positions in the 'rowNumbers' array
  /// for the rows pointed to by 'rows'. The values are copied into the 'result'
  /// vector at the offset pointed by 'resultOffset'. If an entry in 'rows'
  /// is null, sets corresponding row in 'result' to null. The positions in
  /// 'rowNumbers' array can repeat and also appear out of order. If rowNumbers
  /// has a negative value, then the corresponding row in 'result' is set to
  /// null.
  void extractColumn(
      const char* const* rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t columnIndex,
      const vector_size_t resultOffset,
      const VectorPtr& result) const {
    extractColumn(
        rows,
        rowNumbers,
        columnAt(columnIndex),
        columnHasNulls(columnIndex),
        resultOffset,
        result);
  }

  /// Sets in result all locations with null values in columnIndex for rows.
  void extractNulls(
      const char* const* rows,
      int32_t numRows,
      int32_t columnIndex,
      const BufferPtr& result) const {
    extractNulls(rows, numRows, columnAt(columnIndex), result);
  }

  /// Copies the 'probed' flags for the specified rows into 'result'.
  /// The 'result' is expected to be flat vector of type boolean.
  /// For rows with null keys, sets null in 'result' if 'setNullForNullKeysRow'
  /// is true and false otherwise. For rows with 'false' probed flag, sets null
  /// in 'result' if 'setNullForNonProbedRow' is true and false otherwise. This
  /// is used for null aware and regular right semi project join types.
  void extractProbedFlags(
      const char* const* rows,
      int32_t numRows,
      bool setNullForNullKeysRow,
      bool setNullForNonProbedRow,
      const VectorPtr& result) const;

  static inline int32_t nullByte(int32_t nullOffset) {
    return nullOffset / 8;
  }

  static inline uint8_t nullMask(int32_t nullOffset) {
    return 1 << (nullOffset & 7);
  }

  /// Only accumulators have initialized flags. accumulatorFlagsOffset is the
  /// offset at which the flags for an accumulator begin. Currently this is the
  /// null flag, followed by the initialized flag. So it's equivalent to the
  /// nullOffset.
  ///
  /// It's guaranteed that the flags for an accumulator appear in the same byte.
  static inline int32_t initializedByte(int32_t accumulatorFlagsOffset) {
    return nullByte(accumulatorFlagsOffset);
  }

  /// accumulatorFlagsOffset is the offset at which the flags for an accumulator
  /// begin.
  static inline int32_t initializedMask(int32_t accumulatorFlagsOffset) {
    return nullMask(accumulatorFlagsOffset) << 1;
  }

  /// No tsan because probed flags may have been set by a different thread.
  /// There is a barrier but tsan does not know this.
  enum class ProbeType { kAll, kProbed, kNotProbed };

  template <ProbeType probeType>
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
  __attribute__((__no_sanitize__("thread")))
#endif
#endif
  int32_t
  listRows(
      RowContainerIterator* iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char** rows) const {
    int32_t count = 0;
    uint64_t totalBytes = 0;
    auto numAllocations = rows_.numRanges();
    if (iter->allocationIndex == 0 && iter->rowOffset == 0) {
      iter->normalizedKeysLeft = numRowsWithNormalizedKey_;
      iter->normalizedKeySize = originalNormalizedKeySize_;
    }
    int32_t rowSize = fixedRowSize_ +
        (iter->normalizedKeysLeft > 0 ? originalNormalizedKeySize_ : 0);
    for (auto i = iter->allocationIndex; i < numAllocations; ++i) {
      auto range = rows_.rangeAt(i);
      auto* data =
          range.data() + memory::alignmentPadding(range.data(), alignment_);
      auto limit = range.size() -
          (reinterpret_cast<uintptr_t>(data) -
           reinterpret_cast<uintptr_t>(range.data()));
      auto row = iter->rowOffset;
      while (row + rowSize <= limit) {
        rows[count++] = data + row +
            (iter->normalizedKeysLeft > 0 ? originalNormalizedKeySize_ : 0);
        VELOX_DCHECK_EQ(
            reinterpret_cast<uintptr_t>(rows[count - 1]) % alignment_, 0);
        row += rowSize;
        auto newTotalBytes = totalBytes + rowSize;
        if (--iter->normalizedKeysLeft == 0) {
          rowSize -= originalNormalizedKeySize_;
        }
        if (bits::isBitSet(rows[count - 1], freeFlagOffset_)) {
          --count;
          continue;
        }
        if constexpr (probeType == ProbeType::kNotProbed) {
          if (bits::isBitSet(rows[count - 1], probedFlagOffset_)) {
            --count;
            continue;
          }
        }
        if constexpr (probeType == ProbeType::kProbed) {
          if (not(bits::isBitSet(rows[count - 1], probedFlagOffset_))) {
            --count;
            continue;
          }
        }
        totalBytes = newTotalBytes;
        if (rowSizeOffset_) {
          totalBytes += variableRowSize(rows[count - 1]);
        }
        if (count == maxRows || totalBytes > maxBytes) {
          iter->rowOffset = row;
          iter->allocationIndex = i;
          return count;
        }
      }
      iter->rowOffset = 0;
    }
    iter->allocationIndex = std::numeric_limits<int32_t>::max();
    return count;
  }

  /// Extracts up to 'maxRows' rows starting at the position of 'iter'. A
  /// default constructed or reset iter starts at the beginning. Returns the
  /// number of rows written to 'rows'. Returns 0 when at end. Stops after the
  /// total size of returned rows exceeds maxBytes.
  int32_t listRows(
      RowContainerIterator* iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char** rows) const {
    return listRows<ProbeType::kAll>(iter, maxRows, maxBytes, rows);
  }

  int32_t listRows(RowContainerIterator* iter, int32_t maxRows, char** rows)
      const {
    return listRows<ProbeType::kAll>(iter, maxRows, kUnlimited, rows);
  }

  /// Sets 'probed' flag for the specified rows. Used by the right and
  /// full join to mark build-side rows that matches join
  /// condition. 'rows' may contain duplicate entries for the cases
  /// where single probe row matched multiple build rows. In case of
  /// the full join, 'rows' may include null entries that correspond
  /// to probe rows with no match. No tsan because any thread can set
  /// this without synchronization. There is a barrier between setting
  /// and reading.
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
  __attribute__((__no_sanitize__("thread")))
#endif
#endif
  void
  setProbedFlag(char** rows, int32_t numRows);

  /// Returns true if 'row' at 'column' equals the value at 'index' in
  /// 'decoded'. 'mayHaveNulls' specifies if nulls need to be checked. This is a
  /// fast path for compare().
  template <bool mayHaveNulls>
  bool equals(
      const char* row,
      RowColumn column,
      const DecodedVector& decoded,
      vector_size_t index) const;

  /// Compares the value at 'column' in 'row' with the value at 'index' in
  /// 'decoded'. Returns 0 for equal, < 0 for 'row' < 'decoded', > 0 otherwise.
  int32_t compare(
      const char* row,
      RowColumn column,
      const DecodedVector& decoded,
      vector_size_t index,
      CompareFlags flags = CompareFlags()) const;

  /// Compares the value at 'columnIndex' between 'left' and 'right'. Returns
  /// 0 for equal, < 0 for left < right, > 0 otherwise.
  int32_t compare(
      const char* left,
      const char* right,
      int32_t columnIndex,
      CompareFlags flags = CompareFlags()) const;

  /// Compares the value between 'left' at 'leftIndex' and 'right' and
  /// 'rightIndex'. Returns 0 for equal, < 0 for left < right, > 0 otherwise.
  /// Both columns should have the same type.
  int32_t compare(
      const char* left,
      const char* right,
      int leftColumnIndex,
      int rightColumnIndex,
      CompareFlags flags = CompareFlags()) const;

  /// Allows get/set of the normalized key. If normalized keys are used, they
  /// are stored in the word immediately below the hash table row.
  static inline normalized_key_t& normalizedKey(char* group) {
    return reinterpret_cast<normalized_key_t*>(group)[-1];
  }

  void disableNormalizedKeys() {
    normalizedKeySize_ = 0;
  }

  RowColumn columnAt(int32_t index) const {
    return rowColumns_[index];
  }

  /// Returns the size of a string or complex types value stored in the
  /// specified row and column.
  int32_t variableSizeAt(const char* row, column_index_t column) const;

  /// Returns the per row size of a fixed size column.
  int32_t fixedSizeAt(column_index_t column) const;

  /// Bit offset of the probed flag for a full or right outer join  payload.
  /// 0 if not applicable.
  int32_t probedFlagOffset() const {
    return probedFlagOffset_;
  }

  /// Returns the offset of a uint32_t row size or 0 if the row has no variable
  /// width fields or accumulators.
  int32_t rowSizeOffset() const {
    return rowSizeOffset_;
  }

  /// For a hash join table with possible non-unique entries, the offset of the
  /// pointer to the next row with the same key. 0 if keys are guaranteed
  /// unique, e.g. for a group by or semijoin build.
  int32_t nextOffset() const {
    return nextOffset_;
  }

  /// Creates a next-row-vector if it doesn't exist. Appends the row address to
  /// the next-row-vector, and store the address of the next-row-vector in the
  /// 'nextOffset_' slot for all duplicate rows.
  void appendNextRow(char* current, char* nextRow);

  /// Hashes the values of 'columnIndex' for 'rows'.  If 'mix' is true, mixes
  /// the hash with the existing value in 'result'.
  void hash(
      int32_t columnIndex,
      folly::Range<char**> rows,
      bool mix,
      uint64_t* result) const;

  uint64_t allocatedBytes() const {
    return rows_.allocatedBytes() + stringAllocator_->retainedSize();
  }

  /// Returns the number of fixed size rows that can be allocated without
  /// growing the container and the number of unused bytes of reserved storage
  /// for variable length data.
  std::pair<uint64_t, uint64_t> freeSpace() const {
    return std::make_pair<uint64_t, uint64_t>(
        rows_.freeBytes() / fixedRowSize_ + numFreeRows_,
        stringAllocator_->freeSpace());
  }

  /// Returns the average size of rows in bytes stored in this container.
  std::optional<int64_t> estimateRowSize() const;

  /// Returns a cap on extra memory that may be needed when adding 'numRows'
  /// and variableLengthBytes of out-of-line variable length data.
  int64_t sizeIncrement(vector_size_t numRows, int64_t variableLengthBytes)
      const;

  /// Resets the state to be as after construction. Frees memory for payload.
  void clear();

  int32_t compareRows(
      const char* left,
      const char* right,
      const std::vector<CompareFlags>& flags = {}) const {
    VELOX_DCHECK(flags.empty() || flags.size() == keyTypes_.size());
    for (auto i = 0; i < keyTypes_.size(); ++i) {
      auto result =
          compare(left, right, i, flags.empty() ? CompareFlags() : flags[i]);
      if (result) {
        return result;
      }
    }
    return 0;
  }

  memory::MemoryPool* pool() const {
    return stringAllocator_->pool();
  }

  /// Returns the types of all non-aggregate columns of 'this', keys first.
  const auto& columnTypes() const {
    return types_;
  }

  /// Returns the aggregated column stats of the column with given
  /// 'columnIndex'. nullopt will be returned if the column stats was previous
  /// invalidated. Any row erase operations will invalidate column stats.
  std::optional<RowColumn::Stats> columnStats(int32_t columnIndex) const;

  uint32_t columnNullCount(int32_t columnIndex) const {
    return rowColumnsStats_[columnIndex].nullCount();
  }

  const auto& keyTypes() const {
    return keyTypes_;
  }

  /// Returns true if specified column has nulls, false otherwise.
  inline bool columnHasNulls(int32_t columnIndex) const {
    return columnStats(columnIndex)->numCells() > 0 &&
        columnStats(columnIndex)->nullCount() > 0;
  }

  const std::vector<Accumulator>& accumulators() const {
    return accumulators_;
  }

  const HashStringAllocator& stringAllocator() const {
    return *stringAllocator_;
  }

  static inline bool
  isNullAt(const char* row, int32_t nullByte, uint8_t nullMask) {
    return (row[nullByte] & nullMask) != 0;
  }

  static inline bool isNullAt(const char* row, const RowColumn& rowColumn) {
    return (row[rowColumn.nullByte()] & rowColumn.nullMask()) != 0;
  }

  /// Returns true if the value at rowColumn in row is NaN.
  template <
      typename T,
      std::enable_if_t<std::is_floating_point_v<T>, int32_t> = 0>
  static inline bool isNanAt(const char* row, const RowColumn& rowColumn) {
    if (isNullAt(row, rowColumn.nullByte(), rowColumn.nullMask())) {
      return false;
    }
    return std::isnan(valueAt<T>(row, rowColumn.offset()));
  }

  /// Creates a container to store a partition number for each row in this row
  /// container. This is used by parallel join build which is responsible for
  /// filling this. This function also marks this row container as immutable
  /// after this call, we expect the user only call this once.
  std::unique_ptr<RowPartitions> createRowPartitions(memory::MemoryPool& pool);

  /// Retrieves rows from 'iterator' whose partition equals 'partition'. Writes
  /// up to 'maxRows' pointers to the rows in 'result'. 'rowPartitions' contains
  /// the partition number of each row in this container. The function returns
  /// the number of rows retrieved, 0 when no more rows are found. 'iterator' is
  /// expected to be in initial state on first call.
  int32_t listPartitionRows(
      RowContainerIterator& iterator,
      uint8_t partition,
      int32_t maxRows,
      const RowPartitions& rowPartitions,
      char** result) const;

  /// Advances 'iterator' by 'numRows'. The current row after skip is
  /// in iter.currentRow(). This is null if past end. Public for testing.
  void skip(RowContainerIterator& iterator, int32_t numRows) const;

  bool testingMutable() const {
    return mutable_;
  }

  /// Returns a summary of the container: key types, dependent types, number of
  /// accumulators and number of rows.
  std::string toString() const;

  /// Returns a string representation of the specified row in the same format as
  /// BaseVector::toString(index).
  std::string toString(const char* row) const;

 private:
  // Offset of the pointer to the next free row on a free row.
  static constexpr int32_t kNextFreeOffset = 0;

  template <typename T>
  static inline T valueAt(const char* group, int32_t offset) {
    return *reinterpret_cast<const T*>(group + offset);
  }

  template <typename T>
  static inline T& valueAt(char* group, int32_t offset) {
    return *reinterpret_cast<T*>(group + offset);
  }

  // Copies a string or complex type value from the specified row and column
  // into provided buffer. Stored the size of the data in the first 4 bytes of
  // the buffer. If the value is null, writes zero into the first 4 bytes of
  // destination and returns.
  // @return The number of bytes written to 'destination' including the 4 bytes
  // of the size.
  int32_t extractVariableSizeAt(
      const char* row,
      column_index_t column,
      char* output) const;

  // Copies a string or complex type value from 'data' into the specified row
  // and column. Expects first 4 bytes in 'data' to contain the size of the
  // string or complex value.
  // @return The number of bytes read from 'data': 4 bytes for size + that many
  // bytes.
  int32_t
  storeVariableSizeAt(const char* data, char* row, column_index_t column);

  template <TypeKind Kind>
  static void extractColumnTyped(
      const char* const* rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      RowColumn column,
      bool columnHasNulls,
      int32_t resultOffset,
      const VectorPtr& result) {
    if (rowNumbers.size() > 0) {
      extractColumnTypedInternal<true, Kind>(
          rows,
          rowNumbers,
          rowNumbers.size(),
          column,
          columnHasNulls,
          resultOffset,
          result);
    } else {
      extractColumnTypedInternal<false, Kind>(
          rows,
          rowNumbers,
          numRows,
          column,
          columnHasNulls,
          resultOffset,
          result);
    }
  }

  template <bool useRowNumbers, TypeKind Kind>
  static void extractColumnTypedInternal(
      const char* const* rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      RowColumn column,
      bool columnHasNulls,
      int32_t resultOffset,
      const VectorPtr& result) {
    // Resize the result vector before all copies.
    result->resize(numRows + resultOffset);

    if constexpr (
        Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
        Kind == TypeKind::MAP) {
      extractComplexType<useRowNumbers>(
          rows, rowNumbers, numRows, column, resultOffset, result);
      return;
    }
    using T = typename KindToFlatVector<Kind>::HashRowType;
    auto* flatResult = result->as<FlatVector<T>>();
    auto nullMask = column.nullMask();
    auto offset = column.offset();
    if (!nullMask || !columnHasNulls) {
      extractValuesNoNulls<useRowNumbers, T>(
          rows, rowNumbers, numRows, offset, resultOffset, flatResult);
    } else {
      extractValuesWithNulls<useRowNumbers, T>(
          rows,
          rowNumbers,
          numRows,
          offset,
          column.nullByte(),
          nullMask,
          resultOffset,
          flatResult);
    }
  }

  /// Removes or updates the column stats of a given row by updating each column
  /// stats.
  /// @param row - Points to the row to be removed or updated.
  /// @param setToNull - If true, the row stats is set to a null row,
  /// otherwise, the stats is erased from the columns stats.
  void removeOrUpdateRowColumnStats(const char* row, bool setToNull);

  char*& nextFree(char* row) const {
    return *reinterpret_cast<char**>(row + kNextFreeOffset);
  }

  uint32_t& variableRowSize(char* row) const {
    VELOX_DCHECK(rowSizeOffset_);
    return *reinterpret_cast<uint32_t*>(row + rowSizeOffset_);
  }

  template <TypeKind Kind>
  inline void storeWithNulls(
      const DecodedVector& decoded,
      vector_size_t rowIndex,
      bool isKey,
      char* row,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      int32_t columnIndex) {
    using T = typename TypeTraits<Kind>::NativeType;
    if (decoded.isNullAt(rowIndex)) {
      row[nullByte] |= nullMask;
      // Do not leave an uninitialized value in the case of a
      // null. This is an error with valgrind/asan.
      *reinterpret_cast<T*>(row + offset) = T();
      return;
    }
    if constexpr (std::is_same_v<T, StringView>) {
      RowSizeTracker tracker(row[rowSizeOffset_], *stringAllocator_);
      stringAllocator_->copyMultipart(
          decoded.valueAt<T>(rowIndex), row, offset);
    } else {
      *reinterpret_cast<T*>(row + offset) = decoded.valueAt<T>(rowIndex);
    }
  }

  template <TypeKind Kind>
  inline void storeNoNulls(
      const DecodedVector& decoded,
      vector_size_t rowIndex,
      bool isKey,
      char* row,
      int32_t offset) {
    using T = typename TypeTraits<Kind>::NativeType;
    if constexpr (std::is_same_v<T, StringView>) {
      RowSizeTracker tracker(row[rowSizeOffset_], *stringAllocator_);
      stringAllocator_->copyMultipart(
          decoded.valueAt<T>(rowIndex), row, offset);
    } else {
      *reinterpret_cast<T*>(row + offset) = decoded.valueAt<T>(rowIndex);
    }
  }

  template <TypeKind Kind>
  inline void storeWithNullsBatch(
      const DecodedVector& decoded,
      folly::Range<char**> rows,
      bool isKey,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      int32_t column) {
    for (int32_t i = 0; i < rows.size(); ++i) {
      storeWithNulls<Kind>(
          decoded, i, isKey, rows[i], offset, nullByte, nullMask, column);
      updateColumnStats(decoded, i, rows[i], column);
    }
  }

  template <TypeKind Kind>
  inline void storeNoNullsBatch(
      const DecodedVector& decoded,
      folly::Range<char**> rows,
      bool isKey,
      int32_t offset,
      int32_t column) {
    for (int32_t i = 0; i < rows.size(); ++i) {
      storeNoNulls<Kind>(decoded, i, isKey, rows[i], offset);
      updateColumnStats(decoded, i, rows[i], column);
    }
  }

  template <bool useRowNumbers, typename T>
  static void extractValuesWithNulls(
      const char* const* rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      int32_t resultOffset,
      FlatVector<T>* result) {
    auto maxRows = numRows + resultOffset;
    VELOX_DCHECK_LE(maxRows, result->size());

    BufferPtr& nullBuffer = result->mutableNulls(maxRows, true);
    auto nulls = nullBuffer->asMutable<uint64_t>();
    BufferPtr valuesBuffer = result->mutableValues();
    [[maybe_unused]] auto values = valuesBuffer->asMutableRange<T>();
    for (int32_t i = 0; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        auto rowNumber = rowNumbers[i];
        row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
      } else {
        row = rows[i];
      }
      auto resultIndex = resultOffset + i;
      if (row == nullptr || isNullAt(row, nullByte, nullMask)) {
        bits::setNull(nulls, resultIndex, true);
      } else {
        bits::setNull(nulls, resultIndex, false);
        if constexpr (std::is_same_v<T, StringView>) {
          extractString(valueAt<StringView>(row, offset), result, resultIndex);
        } else {
          values[resultIndex] = valueAt<T>(row, offset);
        }
      }
    }
  }

  template <bool useRowNumbers, typename T>
  static void extractValuesNoNulls(
      const char* const* rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t offset,
      int32_t resultOffset,
      FlatVector<T>* result) {
    [[maybe_unused]] auto maxRows = numRows + resultOffset;
    VELOX_DCHECK_LE(maxRows, result->size());
    BufferPtr valuesBuffer = result->mutableValues();
    [[maybe_unused]] auto values = valuesBuffer->asMutableRange<T>();
    for (int32_t i = 0; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        auto rowNumber = rowNumbers[i];
        row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
      } else {
        row = rows[i];
      }
      auto resultIndex = resultOffset + i;
      if (row == nullptr) {
        result->setNull(resultIndex, true);
      } else {
        result->setNull(resultIndex, false);
        if constexpr (std::is_same_v<T, StringView>) {
          extractString(valueAt<StringView>(row, offset), result, resultIndex);
        } else {
          values[resultIndex] = valueAt<T>(row, offset);
        }
      }
    }
  }

  static HashStringAllocator::InputStream prepareRead(
      const char* row,
      int32_t offset);

  template <bool typeProvidesCustomComparison, TypeKind Kind>
  void hashTyped(
      const Type* type,
      RowColumn column,
      bool nullable,
      folly::Range<char**> rows,
      bool mix,
      uint64_t* result) const;

  template <bool typeProvidesCustomComparison, TypeKind Kind>
  inline bool equalsWithNulls(
      const char* row,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      const DecodedVector& decoded,
      vector_size_t index) const {
    bool rowIsNull = isNullAt(row, nullByte, nullMask);
    bool indexIsNull = decoded.isNullAt(index);
    if (rowIsNull || indexIsNull) {
      return rowIsNull == indexIsNull;
    }

    return equalsNoNulls<typeProvidesCustomComparison, Kind>(
        row, offset, decoded, index);
  }

  template <bool typeProvidesCustomComparison, TypeKind Kind>
  inline bool equalsNoNulls(
      const char* row,
      int32_t offset,
      const DecodedVector& decoded,
      vector_size_t index) const {
    using T = typename KindToFlatVector<Kind>::HashRowType;

    if constexpr (
        Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
        Kind == TypeKind::MAP) {
      return compareComplexType(row, offset, decoded, index) == 0;
    } else if constexpr (
        Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
      return compareStringAsc(
                 valueAt<StringView>(row, offset), decoded, index) == 0;
    } else if constexpr (typeProvidesCustomComparison) {
      return SimpleVector<T>::template comparePrimitiveAscWithCustomComparison<
                 Kind>(
                 decoded.base()->type().get(),
                 decoded.valueAt<T>(index),
                 valueAt<T>(row, offset)) == 0;
    } else {
      return SimpleVector<T>::comparePrimitiveAsc(
                 decoded.valueAt<T>(index), valueAt<T>(row, offset)) == 0;
    }
  }

  template <
      bool typeProvidesCustomComparison,
      TypeKind Kind,
      std::enable_if_t<Kind != TypeKind::OPAQUE, int32_t> = 0>
  inline int compare(
      const char* row,
      RowColumn column,
      const DecodedVector& decoded,
      vector_size_t index,
      CompareFlags flags) const {
    using T = typename KindToFlatVector<Kind>::HashRowType;
    bool rowIsNull = isNullAt(row, column.nullByte(), column.nullMask());
    bool indexIsNull = decoded.isNullAt(index);
    if (rowIsNull) {
      return indexIsNull ? 0 : flags.nullsFirst ? -1 : 1;
    }
    if (indexIsNull) {
      return flags.nullsFirst ? 1 : -1;
    }
    if constexpr (
        Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
        Kind == TypeKind::MAP) {
      return compareComplexType(row, column.offset(), decoded, index, flags);
    } else if constexpr (
        Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
      auto result = compareStringAsc(
          valueAt<StringView>(row, column.offset()), decoded, index);
      return flags.ascending ? result : result * -1;
    } else {
      auto left = valueAt<T>(row, column.offset());
      auto right = decoded.valueAt<T>(index);

      int result;
      if constexpr (typeProvidesCustomComparison) {
        result =
            SimpleVector<T>::template comparePrimitiveAscWithCustomComparison<
                Kind>(decoded.base()->type().get(), left, right);
      } else {
        result = SimpleVector<T>::comparePrimitiveAsc(left, right);
      }

      return flags.ascending ? result : result * -1;
    }
  }

  template <
      bool typeProvidesCustomComparison,
      TypeKind Kind,
      std::enable_if_t<Kind == TypeKind::OPAQUE, int32_t> = 0>
  inline int compare(
      const char* /*row*/,
      RowColumn /*column*/,
      const DecodedVector& /*decoded*/,
      vector_size_t /*index*/,
      CompareFlags /*flags*/) const {
    VELOX_UNSUPPORTED("Comparing Opaque types is not supported.");
  }

  template <
      bool typeProvidesCustomComparison,
      TypeKind Kind,
      std::enable_if_t<Kind != TypeKind::OPAQUE, int32_t> = 0>
  inline int compare(
      const char* left,
      const char* right,
      const Type* type,
      RowColumn leftColumn,
      RowColumn rightColumn,
      CompareFlags flags) const {
    using T = typename KindToFlatVector<Kind>::HashRowType;
    bool leftIsNull =
        isNullAt(left, leftColumn.nullByte(), leftColumn.nullMask());
    bool rightIsNull =
        isNullAt(right, rightColumn.nullByte(), rightColumn.nullMask());
    if (leftIsNull) {
      return rightIsNull ? 0 : flags.nullsFirst ? -1 : 1;
    }
    if (rightIsNull) {
      return flags.nullsFirst ? 1 : -1;
    }

    auto leftOffset = leftColumn.offset();
    auto rightOffset = rightColumn.offset();
    if constexpr (
        Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
        Kind == TypeKind::MAP) {
      return compareComplexType(
          left, right, type, leftOffset, rightOffset, flags);
    } else if constexpr (
        Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
      auto leftValue = valueAt<StringView>(left, leftOffset);
      auto rightValue = valueAt<StringView>(right, rightOffset);
      auto result = compareStringAsc(leftValue, rightValue);
      return flags.ascending ? result : result * -1;
    } else {
      auto leftValue = valueAt<T>(left, leftOffset);
      auto rightValue = valueAt<T>(right, rightOffset);

      int result;
      if constexpr (typeProvidesCustomComparison) {
        result =
            SimpleVector<T>::template comparePrimitiveAscWithCustomComparison<
                Kind>(type, leftValue, rightValue);
      } else {
        result = SimpleVector<T>::comparePrimitiveAsc(leftValue, rightValue);
      }

      return flags.ascending ? result : result * -1;
    }
  }

  template <
      bool typeProvidesCustomComparison,
      TypeKind Kind,
      std::enable_if_t<Kind == TypeKind::OPAQUE, int32_t> = 0>
  inline int compare(
      const char* /*left*/,
      const char* /*right*/,
      const Type* /*type*/,
      RowColumn /*leftColumn*/,
      RowColumn /*rightColumn*/,
      CompareFlags /*flags*/) const {
    VELOX_UNSUPPORTED("Comparing Opaque types is not supported.");
  }

  template <bool typeProvidesCustomComparison, TypeKind Kind>
  inline int compare(
      const char* left,
      const char* right,
      const Type* type,
      RowColumn column,
      CompareFlags flags) const {
    return compare<typeProvidesCustomComparison, Kind>(
        left, right, type, column, column, flags);
  }

  void storeComplexType(
      const DecodedVector& decoded,
      vector_size_t index,
      bool isKey,
      char* row,
      int32_t offset,
      int32_t nullByte = 0,
      uint8_t nullMask = 0,
      int32_t column = 0);

  template <bool useRowNumbers>
  static void extractComplexType(
      const char* const* rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      RowColumn column,
      int32_t resultOffset,
      const VectorPtr& result) {
    auto nullByte = column.nullByte();
    auto nullMask = column.nullMask();
    auto offset = column.offset();

    VELOX_DCHECK_LE(numRows + resultOffset, result->size());
    for (int i = 0; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        auto rowNumber = rowNumbers[i];
        row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
      } else {
        row = rows[i];
      }
      auto resultIndex = resultOffset + i;
      if (!row || isNullAt(row, nullByte, nullMask)) {
        result->setNull(resultIndex, true);
      } else {
        auto stream = prepareRead(row, offset);
        ContainerRowSerde::deserialize(stream, resultIndex, result.get());
      }
    }
  }

  static void extractString(
      StringView value,
      FlatVector<StringView>* values,
      vector_size_t index);

  static int32_t compareStringAsc(
      StringView left,
      const DecodedVector& decoded,
      vector_size_t index);

  static int32_t compareStringAsc(StringView left, StringView right);

  int32_t compareComplexType(
      const char* row,
      int32_t offset,
      const DecodedVector& decoded,
      vector_size_t index,
      CompareFlags flags = CompareFlags()) const;

  int32_t compareComplexType(
      const char* left,
      const char* right,
      const Type* type,
      int32_t offset,
      CompareFlags flags) const;

  int32_t compareComplexType(
      const char* left,
      const char* right,
      const Type* type,
      int32_t leftOffset,
      int32_t rightOffset,
      CompareFlags flags = CompareFlags()) const;

  // Free variable-width fields at column `column_index` associated with the
  // 'rows'. `FieldType` is the type of data representation of the fields in
  // row, and can be one of StringView(represents VARCHAR) and
  // std::string_view(represents ARRAY, MAP or ROW).
  template <typename FieldType>
  void freeVariableWidthFieldsAtColumn(
      size_t column_index,
      folly::Range<char**> rows) {
    static_assert(
        std::is_same_v<FieldType, StringView> ||
        std::is_same_v<FieldType, std::string_view>);

    const auto column = columnAt(column_index);
    for (auto row : rows) {
      if (isNullAt(row, column.nullByte(), column.nullMask())) {
        continue;
      }

      auto& view = valueAt<FieldType>(row, column.offset());
      if constexpr (std::is_same_v<FieldType, StringView>) {
        if (view.isInline()) {
          continue;
        }
      } else {
        if (view.empty()) {
          continue;
        }
      }
      stringAllocator_->free(HashStringAllocator::headerOf(view.data()));
    }
  }

  // Free any variable-width fields associated with the 'rows' and zero out
  // complex-typed field in 'rows'.
  void freeVariableWidthFields(folly::Range<char**> rows);

  // Free any aggregates associated with the 'rows'.
  void freeAggregates(folly::Range<char**> rows);

  void freeRowsExtraMemory(folly::Range<char**> rows);

  inline void updateColumnStats(
      const DecodedVector& decoded,
      vector_size_t rowIndex,
      char* row,
      int32_t columnIndex);

  // Updates column stats for serialized row.
  inline void updateColumnStats(char* row, int32_t columnIndex);

  // Min/max column stats do not support row erasures. This
  // method is called whenever a row is erased.
  void invalidateMinMaxColumnStats() {
    for (auto columnStats : rowColumnsStats_) {
      columnStats.invalidateMinMaxColumnStats();
    }
  }

  const std::vector<TypePtr> keyTypes_;
  const bool nullableKeys_;
  const bool isJoinBuild_;
  // True if normalized keys are enabled in initial state.
  const bool hasNormalizedKeys_;

  const std::unique_ptr<HashStringAllocator> stringAllocator_;

  // Indicates if we can add new row to this row container. It is set to false
  // after user calls 'getRowPartitions()' to create 'rowPartitions' object for
  // parallel join build.
  bool mutable_{true};

  std::vector<Accumulator> accumulators_;

  bool usesExternalMemory_ = false;
  // Types of non-aggregate columns. Keys first. Corresponds pairwise
  // to 'typeKinds_' and 'rowColumns_'.
  std::vector<TypePtr> types_;
  std::vector<TypeKind> typeKinds_;
  int32_t nextOffset_ = 0;
  // Indicates if this row container has rows with duplicate keys. This only
  // applies if 'nextOffset_' is set.
  tsan_atomic<bool> hasDuplicateRows_{false};
  // Bit position of null bit in the row. 0 if no null flag. Order is keys,
  // accumulators, dependent.
  std::vector<int32_t> nullOffsets_;
  // Position of field or accumulator. Corresponds 1:1 to 'nullOffset_'.
  std::vector<int32_t> offsets_;
  // Offset and null indicator offset of non-aggregate fields as a single word.
  // Corresponds pairwise to 'types_'.
  std::vector<RowColumn> rowColumns_;
  // Aggregated column stats(e.g. min/max size) for non-aggregate
  // fields. Index aligns with 'rowColumns_'.
  std::vector<RowColumn::Stats> rowColumnsStats_;
  // Bit offset of the probed flag for a full or right outer join  payload. 0 if
  // not applicable.
  int32_t probedFlagOffset_ = 0;

  // Bit position of free bit.
  int32_t freeFlagOffset_ = 0;
  int32_t rowSizeOffset_ = 0;

  int32_t fixedRowSize_;
  // How many bytes do the flags (null, probed, free) occupy.
  int32_t flagBytes_;
  // The count of entries that have an extra normalized_key_t before the
  // start.
  int64_t numRowsWithNormalizedKey_ = 0;
  // This is the original normalized key size regardless of whether
  // disableNormalizedKeys() is called or not.
  int originalNormalizedKeySize_;
  // Extra bytes to reserve before  each added row for a normalized key. Set to
  // 0 after deciding not to use normalized keys.
  int normalizedKeySize_;
  uint64_t numRows_ = 0;
  // Head of linked list of free rows.
  char* firstFreeRow_ = nullptr;
  uint64_t numFreeRows_ = 0;

  memory::AllocationPool rows_;

  int alignment_ = 1;

  friend class test::RowContainerTestHelper;
};

template <>
inline int128_t RowContainer::valueAt<int128_t>(
    const char* group,
    int32_t offset) {
  return HugeInt::deserialize(group + offset);
}

template <>
inline void RowContainer::storeWithNulls<TypeKind::ROW>(
    const DecodedVector& decoded,
    vector_size_t rowIndex,
    bool isKey,
    char* row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask,
    int32_t columnIndex) {
  storeComplexType(
      decoded, rowIndex, isKey, row, offset, nullByte, nullMask, columnIndex);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::ROW>(
    const DecodedVector& decoded,
    vector_size_t rowIndex,
    bool isKey,
    char* row,
    int32_t offset) {
  storeComplexType(decoded, rowIndex, isKey, row, offset);
}

template <>
inline void RowContainer::storeWithNulls<TypeKind::ARRAY>(
    const DecodedVector& decoded,
    vector_size_t rowIndex,
    bool isKey,
    char* row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask,
    int32_t columnIndex) {
  storeComplexType(
      decoded, rowIndex, isKey, row, offset, nullByte, nullMask, columnIndex);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::ARRAY>(
    const DecodedVector& decoded,
    vector_size_t rowIndex,
    bool isKey,
    char* row,
    int32_t offset) {
  storeComplexType(decoded, rowIndex, isKey, row, offset);
}

template <>
inline void RowContainer::storeWithNulls<TypeKind::MAP>(
    const DecodedVector& decoded,
    vector_size_t rowIndex,
    bool isKey,
    char* row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask,
    int32_t columnIndex) {
  storeComplexType(
      decoded, rowIndex, isKey, row, offset, nullByte, nullMask, columnIndex);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::MAP>(
    const DecodedVector& decoded,
    vector_size_t rowIndex,
    bool isKey,
    char* row,
    int32_t offset) {
  storeComplexType(decoded, rowIndex, isKey, row, offset);
}

template <>
inline void RowContainer::storeWithNulls<TypeKind::HUGEINT>(
    const DecodedVector& decoded,
    vector_size_t rowIndex,
    bool /*isKey*/,
    char* row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask,
    int32_t columnIndex) {
  if (decoded.isNullAt(rowIndex)) {
    row[nullByte] |= nullMask;
    memset(row + offset, 0, sizeof(int128_t));
    return;
  }
  HugeInt::serialize(decoded.valueAt<int128_t>(rowIndex), row + offset);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::HUGEINT>(
    const DecodedVector& decoded,
    vector_size_t rowIndex,
    bool /*isKey*/,
    char* row,
    int32_t offset) {
  HugeInt::serialize(decoded.valueAt<int128_t>(rowIndex), row + offset);
}

template <>
inline void RowContainer::extractColumnTyped<TypeKind::OPAQUE>(
    const char* const* /*rows*/,
    folly::Range<const vector_size_t*> /*rowNumbers*/,
    int32_t /*numRows*/,
    RowColumn /*column*/,
    bool /*columnHasNulls*/,
    int32_t /*resultOffset*/,
    const VectorPtr& /*result*/) {
  VELOX_UNSUPPORTED("RowContainer doesn't support values of type OPAQUE");
}

inline void RowContainer::extractColumn(
    const char* const* rows,
    int32_t numRows,
    RowColumn column,
    bool columnHasNulls,
    int32_t resultOffset,
    const VectorPtr& result) {
  VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnTyped,
      result->typeKind(),
      rows,
      {},
      numRows,
      column,
      columnHasNulls,
      resultOffset,
      result);
}

inline void RowContainer::extractColumn(
    const char* const* rows,
    folly::Range<const vector_size_t*> rowNumbers,
    RowColumn column,
    bool columnHasNulls,
    int32_t resultOffset,
    const VectorPtr& result) {
  VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnTyped,
      result->typeKind(),
      rows,
      rowNumbers,
      rowNumbers.size(),
      column,
      columnHasNulls,
      resultOffset,
      result);
}

inline void RowContainer::extractNulls(
    const char* const* rows,
    int32_t numRows,
    RowColumn column,
    const BufferPtr& result) {
  VELOX_DCHECK(result->size() >= bits::nbytes(numRows));
  auto* rawResult = result->asMutable<uint64_t>();
  bits::fillBits(rawResult, 0, numRows, false);

  auto nullMask = column.nullMask();
  if (!nullMask) {
    return;
  }

  auto nullByte = column.nullByte();
  for (int32_t i = 0; i < numRows; ++i) {
    const char* row = rows[i];
    if (row == nullptr || isNullAt(row, nullByte, nullMask)) {
      bits::setBit(rawResult, i, true);
    }
  }
}

template <bool mayHaveNulls>
inline bool RowContainer::equals(
    const char* row,
    RowColumn column,
    const DecodedVector& decoded,
    vector_size_t index) const {
  auto typeKind = decoded.base()->typeKind();
  if (typeKind == TypeKind::UNKNOWN) {
    return isNullAt(row, column.nullByte(), column.nullMask());
  }

  if constexpr (!mayHaveNulls) {
    return VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
        equalsNoNulls, false, typeKind, row, column.offset(), decoded, index);
  } else {
    return VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
        equalsWithNulls,
        false,
        typeKind,
        row,
        column.offset(),
        column.nullByte(),
        column.nullMask(),
        decoded,
        index);
  }
}

inline int RowContainer::compare(
    const char* row,
    RowColumn column,
    const DecodedVector& decoded,
    vector_size_t index,
    CompareFlags flags) const {
  if (decoded.base()->typeUsesCustomComparison()) {
    return VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH_ALL(
        compare,
        true,
        decoded.base()->typeKind(),
        row,
        column,
        decoded,
        index,
        flags);
  } else {
    return VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH_ALL(
        compare,
        false,
        decoded.base()->typeKind(),
        row,
        column,
        decoded,
        index,
        flags);
  }
}

inline int RowContainer::compare(
    const char* left,
    const char* right,
    int columnIndex,
    CompareFlags flags) const {
  auto type = types_[columnIndex].get();
  if (type->providesCustomComparison()) {
    return VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH_ALL(
        compare,
        true,
        type->kind(),
        left,
        right,
        type,
        columnAt(columnIndex),
        flags);
  } else {
    return VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH_ALL(
        compare,
        false,
        type->kind(),
        left,
        right,
        type,
        columnAt(columnIndex),
        flags);
  }
}

inline int RowContainer::compare(
    const char* left,
    const char* right,
    int leftColumnIndex,
    int rightColumnIndex,
    CompareFlags flags) const {
  auto leftType = types_[leftColumnIndex].get();
  auto rightType = types_[rightColumnIndex].get();
  VELOX_CHECK(leftType->equivalent(*rightType));

  if (leftType->providesCustomComparison()) {
    return VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH_ALL(
        compare,
        true,
        leftType->kind(),
        left,
        right,
        leftType,
        columnAt(leftColumnIndex),
        columnAt(rightColumnIndex),
        flags);
  } else {
    return VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH_ALL(
        compare,
        false,
        leftType->kind(),
        left,
        right,
        leftType,
        columnAt(leftColumnIndex),
        columnAt(rightColumnIndex),
        flags);
  }
}

/// A comparator of rows stored in the RowContainer compatible with
/// std::priority_queue. Uses specified columns and sorting orders for
/// comparison.
class RowComparator {
 public:
  RowComparator(
      const RowTypePtr& rowType,
      const std::vector<core::FieldAccessTypedExprPtr>& sortingKeys,
      const std::vector<core::SortOrder>& sortingOrders,
      RowContainer* rowContainer);

  /// Returns true if lhs < rhs, false otherwise.
  bool operator()(const char* lhs, const char* rhs);

  /// Returns 0 for equal, < 0 for lhs < rhs, > 0 otherwise.
  int compare(const char* lhs, const char* rhs);

  /// Returns true if decodedVectors[index] < other, false otherwise.
  bool operator()(
      const std::vector<DecodedVector>& decodedVectors,
      vector_size_t index,
      const char* other);

  /// Returns 0 for equal, < 0 for decodedVectors[index] < other,
  /// > 0 otherwise.
  int32_t compare(
      const std::vector<DecodedVector>& decodedVectors,
      vector_size_t index,
      const char* other);

 private:
  std::vector<std::pair<column_index_t, core::SortOrder>> keyInfo_;
  RowContainer* rowContainer_;
};

} // namespace facebook::velox::exec
