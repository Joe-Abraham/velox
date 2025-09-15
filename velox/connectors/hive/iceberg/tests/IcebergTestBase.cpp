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

} // namespace facebook::velox::connector::hive::iceberg
