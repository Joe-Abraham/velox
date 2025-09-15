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

#include "velox/connectors/hive/iceberg/tests/IcebergTestBase.h"

#include "connectors/hive/iceberg/IcebergSplit.h"
#include "exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::connector::hive::iceberg {

std::vector<int64_t> IcebergTestBase::makeRandomDeleteValues(
    int32_t maxRowNumber) {
  std::mt19937 gen{0};
  std::vector<int64_t> deleteRows;
  for (int i = 0; i < maxRowNumber; i++) {
    if (folly::Random::rand32(0, 10, gen) > 8) {
      deleteRows.push_back(i);
    }
  }
  return deleteRows;
}

template <typename T>
std::vector<T> IcebergTestBase::makeSequenceValues(
    int32_t numRows,
    int8_t repeat) {
  static_assert(std::is_integral_v<T>, "T must be an integral type");
  VELOX_CHECK_GT(repeat, 0);

  auto maxValue = std::ceil(static_cast<double>(numRows) / repeat);
  std::vector<T> values;
  values.reserve(numRows);
  for (int32_t i = 0; i < maxValue; i++) {
    for (int8_t j = 0; j < repeat; j++) {
      values.push_back(static_cast<T>(i));
    }
  }
  values.resize(numRows);
  return values;
}

template <TypeKind KIND>
std::string IcebergTestBase::makeNotInList(
    const std::vector<typename TypeTraits<KIND>::NativeType>& deleteValues) {
  using T = typename TypeTraits<KIND>::NativeType;
  if (deleteValues.empty()) {
    return "";
  }

  if constexpr (KIND == TypeKind::BOOLEAN) {
    VELOX_FAIL("Unsupported Type : {}", TypeTraits<KIND>::name);
  } else if constexpr (
      KIND == TypeKind::VARCHAR || KIND == TypeKind::VARBINARY) {
    return std::accumulate(
        deleteValues.begin() + 1,
        deleteValues.end(),
        fmt::format("'{}'", to<std::string>(deleteValues[0])),
        [](const std::string& a, const T& b) {
          return a + fmt::format(", '{}'", to<std::string>(b));
        });
  } else if (std::is_integral_v<T> || std::is_floating_point_v<T>) {
    return std::accumulate(
        deleteValues.begin() + 1,
        deleteValues.end(),
        to<std::string>(deleteValues[0]),
        [](const std::string& a, const T& b) {
          return a + ", " + to<std::string>(b);
        });
  } else {
    VELOX_FAIL("Unsupported Type : {}", TypeTraits<KIND>::name);
  }
}

std::vector<std::shared_ptr<ConnectorSplit>> IcebergTestBase::makeIcebergSplits(
    const std::string& dataFilePath,
    const std::vector<IcebergDeleteFile>& deleteFiles,
    const std::unordered_map<std::string, std::optional<std::string>>&
        partitionKeys,
    const uint32_t splitCount) {
  std::unordered_map<std::string, std::string> customSplitInfo;
  customSplitInfo["table_format"] = "hive-iceberg";

  auto file = filesystems::getFileSystem(dataFilePath, nullptr)
                  ->openFileForRead(dataFilePath);
  const int64_t fileSize = file->size();
  std::vector<std::shared_ptr<ConnectorSplit>> splits;
  const uint64_t splitSize = std::floor((fileSize) / splitCount);

  for (int i = 0; i < splitCount; ++i) {
    splits.emplace_back(std::make_shared<HiveIcebergSplit>(
        kHiveConnectorId,
        dataFilePath,
        fileFormat_,
        i * splitSize,
        splitSize,
        partitionKeys,
        std::nullopt,
        customSplitInfo,
        nullptr,
        /*cacheable=*/true,
        deleteFiles));
  }

  return splits;
}

core::PlanNodePtr IcebergTestBase::tableScanNode(
    const RowTypePtr& outputRowType) const {
  return PlanBuilder(pool_.get()).tableScan(outputRowType).planNode();
}

template <TypeKind KIND>
std::vector<RowVectorPtr> IcebergTestBase::makeVectors(
    int32_t count,
    int32_t rowsPerVector,
    int32_t numColumns,
    bool allNulls,
    bool partialNull) {
  using T = typename TypeTraits<KIND>::NativeType;

  // Sample strings for VARCHAR data generation
  const std::vector<std::string> sampleStrings = {
      "apple",     "banana",     "cherry",    "date",       "elderberry",
      "fig",       "grape",      "honeydew",  "kiwi",       "lemon",
      "mango",     "nectarine",  "orange",    "papaya",     "quince",
      "raspberry", "strawberry", "tangerine", "watermelon", "zucchini"};

  std::vector<TypePtr> types;
  for (int j = 0; j < numColumns; j++) {
    types.push_back(createScalarType<KIND>());
  }
  std::vector<std::string> names;
  for (int j = 0; j < numColumns; j++) {
    names.push_back(fmt::format("c{}", j));
  }

  std::vector<RowVectorPtr> rowVectors;
  for (int i = 0; i < count; i++) {
    std::vector<VectorPtr> vectors;

    // Create the column values like below:
    // c0 c1 c2
    //  0  0  0
    //  1  0  0
    //  2  1  0
    //  3  1  1
    //  4  2  1
    //  5  2  1
    //  6  3  2
    // ...
    // In the first column c0, the values are continuously increasing and not
    // repeating. In the second column c1, the values are continuously
    // increasing and each value repeats once. And so on.
    for (int j = 0; j < numColumns; j++) {
      VectorPtr columnVector;

      if (allNulls) {
        // Use allNullFlatVector for all-null columns
        columnVector = vectorMaker_.allNullFlatVector<T>(rowsPerVector);
      } else if constexpr (KIND == TypeKind::VARCHAR) {
        // For VARCHAR, use sample strings with sequence-based indexing
        auto intData = makeSequenceValues<int64_t>(rowsPerVector, j + 1);
        auto stringVector = BaseVector::create<FlatVector<StringView>>(
            VARCHAR(), rowsPerVector, pool_.get());

        for (int idx = 0; idx < rowsPerVector; ++idx) {
          auto stringIndex = intData[idx] % sampleStrings.size();
          const std::string& selectedString = sampleStrings[stringIndex];
          stringVector->set(idx, StringView(selectedString));
        }
        columnVector = stringVector;
      } else if constexpr (KIND == TypeKind::VARBINARY) {
        auto intData = makeSequenceValues<int64_t>(rowsPerVector, j + 1);
        auto binaryVector = BaseVector::create<FlatVector<StringView>>(
            VARBINARY(), rowsPerVector, pool_.get());

        for (int idx = 0; idx < rowsPerVector; ++idx) {
          auto stringIndex = intData[idx] % sampleStrings.size();
          const std::string& baseString = sampleStrings[stringIndex];

          std::string binaryStr;
          for (char c : baseString) {
            binaryStr += static_cast<unsigned char>(c);
          }
          binaryVector->set(idx, StringView(binaryStr));
        }
        columnVector = binaryVector;
      } else if constexpr (std::is_integral_v<T>) {
        auto data = makeSequenceValues<typename TypeTraits<KIND>::NativeType>(
            rowsPerVector, j + 1);
        columnVector = vectorMaker_.flatVector<T>(data);
      } else if constexpr (std::is_floating_point_v<T>) {
        auto intData = makeSequenceValues<int64_t>(rowsPerVector, j + 1);
        std::vector<T> floatData;
        floatData.reserve(intData.size());
        for (auto val : intData) {
          floatData.push_back(static_cast<T>(val) + 0.5f);
        }
        columnVector = vectorMaker_.flatVector<T>(floatData);
      } else {
        VELOX_FAIL(
            "Unsupported type for makeVectors: {}", TypeTraits<KIND>::name);
      }

      // Apply partial nulls by randomly setting some positions to null
      if (partialNull && !allNulls) {
        std::mt19937 gen(42); // Fixed seed for reproducibility
        std::uniform_real_distribution<> dis(0.0, 1.0);
        const double nullProbability = 0.2;

        for (vector_size_t idx = 0; idx < rowsPerVector; ++idx) {
          if (dis(gen) < nullProbability) {
            columnVector->setNull(idx, true);
          }
        }
      }

      vectors.push_back(columnVector);
    }

    rowVectors.push_back(makeRowVector(names, vectors));
  }

  rowType_ = std::make_shared<RowType>(std::move(names), std::move(types));

  return rowVectors;
}

// Explicit template instantiations for makeSequenceValues
template std::vector<bool> IcebergTestBase::makeSequenceValues<bool>(
    int32_t,
    int8_t);
template std::vector<int8_t> IcebergTestBase::makeSequenceValues<int8_t>(
    int32_t,
    int8_t);
template std::vector<int16_t> IcebergTestBase::makeSequenceValues<int16_t>(
    int32_t,
    int8_t);
template std::vector<int32_t> IcebergTestBase::makeSequenceValues<int32_t>(
    int32_t,
    int8_t);
template std::vector<int64_t> IcebergTestBase::makeSequenceValues<int64_t>(
    int32_t,
    int8_t);
template std::vector<int128_t> IcebergTestBase::makeSequenceValues<int128_t>(
    int32_t,
    int8_t);

// Explicit template instantiations for makeNotInList
template std::string IcebergTestBase::makeNotInList<TypeKind::BOOLEAN>(
    const std::vector<bool>&);
template std::string IcebergTestBase::makeNotInList<TypeKind::TINYINT>(
    const std::vector<int8_t>&);
template std::string IcebergTestBase::makeNotInList<TypeKind::SMALLINT>(
    const std::vector<int16_t>&);
template std::string IcebergTestBase::makeNotInList<TypeKind::INTEGER>(
    const std::vector<int32_t>&);
template std::string IcebergTestBase::makeNotInList<TypeKind::BIGINT>(
    const std::vector<int64_t>&);
template std::string IcebergTestBase::makeNotInList<TypeKind::REAL>(
    const std::vector<float>&);
template std::string IcebergTestBase::makeNotInList<TypeKind::DOUBLE>(
    const std::vector<double>&);
template std::string IcebergTestBase::makeNotInList<TypeKind::VARCHAR>(
    const std::vector<StringView>&);
template std::string IcebergTestBase::makeNotInList<TypeKind::VARBINARY>(
    const std::vector<StringView>&);
template std::string IcebergTestBase::makeNotInList<TypeKind::TIMESTAMP>(
    const std::vector<Timestamp>&);
template std::string IcebergTestBase::makeNotInList<TypeKind::HUGEINT>(
    const std::vector<int128_t>&);

// Explicit template instantiations for makeVectors
template std::vector<RowVectorPtr> IcebergTestBase::makeVectors<
    TypeKind::BOOLEAN>(int32_t, int32_t, int32_t, bool, bool);
template std::vector<RowVectorPtr> IcebergTestBase::makeVectors<
    TypeKind::TINYINT>(int32_t, int32_t, int32_t, bool, bool);
template std::vector<RowVectorPtr> IcebergTestBase::makeVectors<
    TypeKind::SMALLINT>(int32_t, int32_t, int32_t, bool, bool);
template std::vector<RowVectorPtr> IcebergTestBase::makeVectors<
    TypeKind::INTEGER>(int32_t, int32_t, int32_t, bool, bool);
template std::vector<RowVectorPtr> IcebergTestBase::makeVectors<
    TypeKind::BIGINT>(int32_t, int32_t, int32_t, bool, bool);
template std::vector<RowVectorPtr> IcebergTestBase::makeVectors<TypeKind::REAL>(
    int32_t,
    int32_t,
    int32_t,
    bool,
    bool);
template std::vector<RowVectorPtr> IcebergTestBase::makeVectors<
    TypeKind::DOUBLE>(int32_t, int32_t, int32_t, bool, bool);
template std::vector<RowVectorPtr> IcebergTestBase::makeVectors<
    TypeKind::VARCHAR>(int32_t, int32_t, int32_t, bool, bool);
template std::vector<RowVectorPtr> IcebergTestBase::makeVectors<
    TypeKind::VARBINARY>(int32_t, int32_t, int32_t, bool, bool);
template std::vector<RowVectorPtr> IcebergTestBase::makeVectors<
    TypeKind::TIMESTAMP>(int32_t, int32_t, int32_t, bool, bool);
template std::vector<RowVectorPtr> IcebergTestBase::makeVectors<
    TypeKind::HUGEINT>(int32_t, int32_t, int32_t, bool, bool);

} // namespace facebook::velox::connector::hive::iceberg
