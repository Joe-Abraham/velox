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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/iceberg/IcebergDeleteFile.h"
#include "velox/connectors/hive/iceberg/IcebergMetadataColumns.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include <folly/Singleton.h>

using namespace facebook::velox::exec::test;
using namespace facebook::velox::exec;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::test;

namespace facebook::velox::connector::hive::iceberg {

class HiveIcebergTest : public HiveConnectorTestBase {
 public:
  HiveIcebergTest()
      : config_{std::make_shared<facebook::velox::dwrf::Config>()} {
    // Make the writers flush per batch so that we can create non-aligned
    // RowGroups between the base data files and delete files
    flushPolicyFactory_ = []() {
      return std::make_unique<dwrf::LambdaFlushPolicy>([]() { return true; });
    };
  }

  /// Create 1 base data file data_file_1 with 2 RowGroups of 10000 rows each.
  /// Also create 1 delete file delete_file_1 which contains delete positions
  /// for data_file_1.
  void assertSingleBaseFileSingleDeleteFile(
      const std::vector<int64_t>& deletePositionsVec) {
    std::map<std::string, std::vector<int64_t>> rowGroupSizesForFiles = {
        {"data_file_1", {10000, 10000}}};
    std::unordered_map<
        std::string,
        std::multimap<std::string, std::vector<int64_t>>>
        deleteFilesForBaseDatafiles = {
            {"delete_file_1", {{"data_file_1", deletePositionsVec}}}};

    assertPositionalDeletes(
        rowGroupSizesForFiles, deleteFilesForBaseDatafiles, 0);
  }

  /// Create 3 base data files, where the first file data_file_0 has 500 rows,
  /// the second file data_file_1 contains 2 RowGroups of 10000 rows each, and
  /// the third file data_file_2 contains 500 rows. It creates 1 positional
  /// delete file delete_file_1, which contains delete positions for
  /// data_file_1.
  void assertMultipleBaseFileSingleDeleteFile(
      const std::vector<int64_t>& deletePositionsVec) {
    int64_t previousFileRowCount = 500;
    int64_t afterFileRowCount = 500;

    assertPositionalDeletes(
        {
            {"data_file_0", {previousFileRowCount}},
            {"data_file_1", {10000, 10000}},
            {"data_file_2", {afterFileRowCount}},
        },
        {{"delete_file_1", {{"data_file_1", deletePositionsVec}}}},
        0);
  }

  /// Create 1 base data file data_file_1 with 2 RowGroups of 10000 rows each.
  /// Create multiple delete files with name data_file_1, data_file_2, and so on
  void assertSingleBaseFileMultipleDeleteFiles(
      const std::vector<std::vector<int64_t>>& deletePositionsVecs) {
    std::map<std::string, std::vector<int64_t>> rowGroupSizesForFiles = {
        {"data_file_1", {10000, 10000}}};

    std::unordered_map<
        std::string,
        std::multimap<std::string, std::vector<int64_t>>>
        deleteFilesForBaseDatafiles;
    for (int i = 0; i < deletePositionsVecs.size(); i++) {
      std::string deleteFileName = fmt::format("delete_file_{}", i);
      deleteFilesForBaseDatafiles[deleteFileName] = {
          {"data_file_1", deletePositionsVecs[i]}};
    }
    assertPositionalDeletes(
        rowGroupSizesForFiles, deleteFilesForBaseDatafiles, 0);
  }

  void assertMultipleSplits(
      const std::vector<int64_t>& deletePositions,
      int32_t fileCount,
      int32_t numPrefetchSplits,
      int rowCountPerFile = rowCount,
      int32_t splitCountPerFile = 1) {
    std::map<std::string, std::vector<int64_t>> rowGroupSizesForFiles;
    for (int32_t i = 0; i < fileCount; i++) {
      std::string dataFileName = fmt::format("data_file_{}", i);
      rowGroupSizesForFiles[dataFileName] = {rowCountPerFile};
    }

    std::unordered_map<
        std::string,
        std::multimap<std::string, std::vector<int64_t>>>
        deleteFilesForBaseDatafiles;
    for (int i = 0; i < fileCount; i++) {
      std::string deleteFileName = fmt::format("delete_file_{}", i);
      deleteFilesForBaseDatafiles[deleteFileName] = {
          {fmt::format("data_file_{}", i), deletePositions}};
    }

    assertPositionalDeletes(
        rowGroupSizesForFiles,
        deleteFilesForBaseDatafiles,
        numPrefetchSplits,
        splitCountPerFile);
  }

  std::vector<int64_t> makeRandomIncreasingValues(int64_t begin, int64_t end) {
    VELOX_CHECK(begin < end);

    std::mt19937 gen{0};
    std::vector<int64_t> values;
    values.reserve(end - begin);
    for (int i = begin; i < end; i++) {
      if (folly::Random::rand32(0, 10, gen) > 8) {
        values.push_back(i);
      }
    }
    return values;
  }

  std::vector<int64_t> makeContinuousIncreasingValues(
      int64_t begin,
      int64_t end) {
    std::vector<int64_t> values;
    values.resize(end - begin);
    std::iota(values.begin(), values.end(), begin);
    return values;
  }

  /// @rowGroupSizesForFiles The key is the file name, and the value is a vector
  /// of RowGroup sizes
  /// @deleteFilesForBaseDatafiles The key is the delete file name, and the
  /// value contains the information about the content of this delete file.
  /// e.g. {
  ///         "delete_file_1",
  ///         {
  ///             {"data_file_1", {1, 2, 3}},
  ///             {"data_file_1", {4, 5, 6}},
  ///             {"data_file_2", {0, 2, 4}}
  ///         }
  ///     }
  /// represents one delete file called delete_file_1, which contains delete
  /// positions for data_file_1 and data_file_2. THere are 3 RowGroups in this
  /// delete file, the first two contain positions for data_file_1, and the last
  /// contain positions for data_file_2
  void assertPositionalDeletes(
      const std::map<std::string, std::vector<int64_t>>& rowGroupSizesForFiles,
      const std::unordered_map<
          std::string,
          std::multimap<std::string, std::vector<int64_t>>>&
          deleteFilesForBaseDatafiles,
      int32_t numPrefetchSplits = 0,
      int32_t splitCount = 1) {
    // Keep the reference to the deleteFilePath, otherwise the corresponding
    // file will be deleted.
    std::map<std::string, std::shared_ptr<TempFilePath>> dataFilePaths =
        writeDataFiles(rowGroupSizesForFiles);
    std::unordered_map<
        std::string,
        std::pair<int64_t, std::shared_ptr<TempFilePath>>>
        deleteFilePaths = writePositionDeleteFiles(
            deleteFilesForBaseDatafiles, dataFilePaths);

    std::vector<std::shared_ptr<ConnectorSplit>> splits;

    for (const auto& dataFile : dataFilePaths) {
      std::string baseFileName = dataFile.first;
      std::string baseFilePath = dataFile.second->getPath();

      std::vector<IcebergDeleteFile> deleteFiles;

      for (auto const& deleteFile : deleteFilesForBaseDatafiles) {
        std::string deleteFileName = deleteFile.first;
        std::multimap<std::string, std::vector<int64_t>> deleteFileContent =
            deleteFile.second;

        if (deleteFileContent.count(baseFileName) != 0) {
          // If this delete file contains rows for the target base file, then
          // add it to the split
          auto deleteFilePath =
              deleteFilePaths[deleteFileName].second->getPath();
          IcebergDeleteFile icebergDeleteFile(
              FileContent::kPositionalDeletes,
              deleteFilePath,
              fileFormat_,
              deleteFilePaths[deleteFileName].first,
              testing::internal::GetFileSize(
                  std::fopen(deleteFilePath.c_str(), "r")));
          deleteFiles.push_back(icebergDeleteFile);
        }
      }

      auto icebergSplits =
          makeIcebergSplits(baseFilePath, deleteFiles, {}, splitCount);
      splits.insert(splits.end(), icebergSplits.begin(), icebergSplits.end());
    }

    std::string duckdbSql =
        getDuckDBQuery(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);
    auto plan = tableScanNode(rowType_);
    auto task = HiveConnectorTestBase::assertQuery(
        plan, splits, duckdbSql, numPrefetchSplits);

    auto planStats = toPlanStats(task->taskStats());
    auto scanNodeId = plan->id();
    auto it = planStats.find(scanNodeId);
    ASSERT_TRUE(it != planStats.end());
    ASSERT_TRUE(it->second.peakMemoryBytes > 0);
  }

  template <typename T>
  void assertEqualityDeletes(
      const std::unordered_map<int8_t, std::vector<std::vector<T>>>&
          equalityDeleteVectorMap,
      const std::unordered_map<int8_t, std::vector<int32_t>>&
          equalityFieldIdsMap,
      std::string duckDbSql = "",
      std::vector<RowVectorPtr> dataVectors = {}) {
    VELOX_CHECK_EQ(equalityDeleteVectorMap.size(), equalityFieldIdsMap.size());
    // We will create data vectors with numColumns number of columns that is the
    // max field Id in equalityFieldIds
    int32_t numDataColumns = 0;

    for (auto it = equalityFieldIdsMap.begin(); it != equalityFieldIdsMap.end();
         ++it) {
      auto equalityFieldIds = it->second;
      auto currentMax =
          *std::max_element(equalityFieldIds.begin(), equalityFieldIds.end());
      numDataColumns = std::max(numDataColumns, currentMax);
    }

    VELOX_CHECK_GT(numDataColumns, 0);
    VELOX_CHECK_GE(numDataColumns, equalityDeleteVectorMap.size());
    VELOX_CHECK_GT(equalityDeleteVectorMap.size(), 0);

    VELOX_CHECK_LE(equalityFieldIdsMap.size(), numDataColumns);

    std::shared_ptr<TempFilePath> dataFilePath =
        writeDataFiles(rowCount, numDataColumns, 1, dataVectors)[0];

    std::vector<IcebergDeleteFile> deleteFiles;
    std::string predicates = "";
    unsigned long numDeletedValues = 0;

    std::vector<std::shared_ptr<TempFilePath>> deleteFilePaths;
    for (auto it = equalityFieldIdsMap.begin();
         it != equalityFieldIdsMap.end();) {
      auto equalityFieldIds = it->second;
      auto equalityDeleteVector = equalityDeleteVectorMap.at(it->first);
      VELOX_CHECK_GT(equalityDeleteVector.size(), 0);
      numDeletedValues =
          std::max(numDeletedValues, equalityDeleteVector[0].size());
      deleteFilePaths.push_back(writeEqualityDeleteFile(equalityDeleteVector));
      IcebergDeleteFile deleteFile(
          FileContent::kEqualityDeletes,
          deleteFilePaths.back()->getPath(),
          fileFormat_,
          equalityDeleteVector[0].size(),
          testing::internal::GetFileSize(
              std::fopen(deleteFilePaths.back()->getPath().c_str(), "r")),
          equalityFieldIds);
      deleteFiles.push_back(deleteFile);
      predicates += makePredicates(equalityDeleteVector, equalityFieldIds);
      ++it;
      if (it != equalityFieldIdsMap.end()) {
        predicates += " AND ";
      }
    }

    // The default split count is 1.
    auto icebergSplits =
        makeIcebergSplits(dataFilePath->getPath(), deleteFiles);

    // If the caller passed in a query, use that.
    if (duckDbSql == "") {
      // Select all columns
      duckDbSql = "SELECT * FROM tmp ";
      if (numDeletedValues > 0) {
        duckDbSql += fmt::format("WHERE {}", predicates);
      }
    }

    assertEqualityDeletes(
        icebergSplits.back(),
        !dataVectors.empty() ? asRowType(dataVectors[0]->type()) : rowType_,
        duckDbSql);

    // Select a column that's not in the filter columns
    if (numDataColumns > 1 &&
        equalityDeleteVectorMap.at(0).size() < numDataColumns) {
      std::string duckDbQuery = "SELECT c0 FROM tmp";
      if (numDeletedValues > 0) {
        duckDbQuery += fmt::format(" WHERE {}", predicates);
      }

      std::vector<std::string> names({"c0"});
      std::vector<TypePtr> types(1, BIGINT());
      assertEqualityDeletes(
          icebergSplits.back(),
          std::make_shared<RowType>(std::move(names), std::move(types)),
          duckDbQuery);
    }
  }

  std::vector<int64_t> makeSequenceValues(int32_t numRows, int8_t repeat = 1) {
    VELOX_CHECK_GT(repeat, 0);

    auto maxValue = std::ceil((double)numRows / repeat);
    std::vector<int64_t> values;
    values.reserve(numRows);
    for (int32_t i = 0; i < maxValue; i++) {
      for (int8_t j = 0; j < repeat; j++) {
        values.push_back(i);
      }
    }
    values.resize(numRows);
    return values;
  }

  std::vector<int64_t> makeRandomDeleteValues(int32_t maxRowNumber) {
    std::mt19937 gen{0};
    std::vector<int64_t> deleteRows;
    for (int i = 0; i < maxRowNumber; i++) {
      if (folly::Random::rand32(0, 10, gen) > 8) {
        deleteRows.push_back(i);
      }
    }
    return deleteRows;
  }

  const static int rowCount = 20000;

 protected:
  std::shared_ptr<dwrf::Config> config_;
  std::function<std::unique_ptr<dwrf::DWRFFlushPolicy>()> flushPolicyFactory_;

  std::vector<std::shared_ptr<ConnectorSplit>> makeIcebergSplits(
      const std::string& dataFilePath,
      const std::vector<IcebergDeleteFile>& deleteFiles = {},
      const std::unordered_map<std::string, std::optional<std::string>>&
          partitionKeys = {},
      const uint32_t splitCount = 1) {
    std::unordered_map<std::string, std::string> customSplitInfo;
    customSplitInfo["table_format"] = "hive-iceberg";

    auto file = filesystems::getFileSystem(dataFilePath, nullptr)
                    ->openFileForRead(dataFilePath);
    const int64_t fileSize = file->size();
    std::vector<std::shared_ptr<ConnectorSplit>> splits;
    const uint64_t splitSize = std::floor((fileSize) / splitCount);

    for (int i = 0; i < splitCount; ++i) {
      splits.emplace_back(
          std::make_shared<HiveIcebergSplit>(
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

  void assertEqualityDeletes(
      std::shared_ptr<connector::ConnectorSplit> split,
      RowTypePtr outputRowType,
      const std::string& duckDbSql) {
    auto plan = tableScanNode(outputRowType);
    auto task = OperatorTestBase::assertQuery(plan, {split}, duckDbSql);

    auto planStats = toPlanStats(task->taskStats());
    auto scanNodeId = plan->id();
    auto it = planStats.find(scanNodeId);
    ASSERT_TRUE(it != planStats.end());
    ASSERT_TRUE(it->second.peakMemoryBytes > 0);
  }

 private:
  std::map<std::string, std::shared_ptr<TempFilePath>> writeDataFiles(
      std::map<std::string, std::vector<int64_t>> rowGroupSizesForFiles) {
    std::map<std::string, std::shared_ptr<TempFilePath>> dataFilePaths;

    std::vector<RowVectorPtr> dataVectorsJoined;
    dataVectorsJoined.reserve(rowGroupSizesForFiles.size());

    int64_t startingValue = 0;
    for (auto& dataFile : rowGroupSizesForFiles) {
      dataFilePaths[dataFile.first] = TempFilePath::create();

      // We make the values are continuously increasing even across base data
      // files. This is to make constructing DuckDB queries easier
      std::vector<RowVectorPtr> dataVectors =
          makeVectors(dataFile.second, startingValue);
      writeToFile(
          dataFilePaths[dataFile.first]->getPath(),
          dataVectors,
          config_,
          flushPolicyFactory_);

      for (int i = 0; i < dataVectors.size(); i++) {
        dataVectorsJoined.push_back(dataVectors[i]);
      }
    }

    createDuckDbTable(dataVectorsJoined);
    return dataFilePaths;
  }

  /// Input is like <"deleteFile1", <"dataFile1", {pos_RG1, pos_RG2,..}>,
  /// <"dataFile2", {pos_RG1, pos_RG2,..}>
  std::unordered_map<
      std::string,
      std::pair<int64_t, std::shared_ptr<TempFilePath>>>
  writePositionDeleteFiles(
      const std::unordered_map<
          std::string, // delete file name
          std::multimap<
              std::string,
              std::vector<int64_t>>>&
          deleteFilesForBaseDatafiles, // <base file name, delete position
                                       // vector for all RowGroups>
      std::map<std::string, std::shared_ptr<TempFilePath>> baseFilePaths) {
    std::unordered_map<
        std::string,
        std::pair<int64_t, std::shared_ptr<TempFilePath>>>
        deleteFilePaths;
    deleteFilePaths.reserve(deleteFilesForBaseDatafiles.size());

    for (auto& deleteFile : deleteFilesForBaseDatafiles) {
      auto deleteFileName = deleteFile.first;
      auto deleteFileContent = deleteFile.second;
      auto deleteFilePath = TempFilePath::create();

      std::vector<RowVectorPtr> deleteFileVectors;
      int64_t totalPositionsInDeleteFile = 0;

      for (auto& deleteFileRowGroup : deleteFileContent) {
        auto baseFileName = deleteFileRowGroup.first;
        auto baseFilePath = baseFilePaths[baseFileName]->getPath();
        auto positionsInRowGroup = deleteFileRowGroup.second;

        auto filePathVector = makeFlatVector<std::string>(
            static_cast<vector_size_t>(positionsInRowGroup.size()),
            [&](vector_size_t row) { return baseFilePath; });
        auto deletePosVector = makeFlatVector<int64_t>(positionsInRowGroup);

        RowVectorPtr deleteFileVector = makeRowVector(
            {pathColumn_->name, posColumn_->name},
            {filePathVector, deletePosVector});

        deleteFileVectors.push_back(deleteFileVector);
        totalPositionsInDeleteFile += positionsInRowGroup.size();
      }

      writeToFile(
          deleteFilePath->getPath(),
          deleteFileVectors,
          config_,
          flushPolicyFactory_);

      deleteFilePaths[deleteFileName] =
          std::make_pair(totalPositionsInDeleteFile, deleteFilePath);
    }

    return deleteFilePaths;
  }

  std::vector<RowVectorPtr> makeVectors(
      std::vector<int64_t> vectorSizes,
      int64_t& startingValue) {
    std::vector<RowVectorPtr> vectors;
    vectors.reserve(vectorSizes.size());

    vectors.reserve(vectorSizes.size());
    for (int j = 0; j < vectorSizes.size(); j++) {
      auto data = makeContinuousIncreasingValues(
          startingValue, startingValue + vectorSizes[j]);
      VectorPtr c0 = makeFlatVector<int64_t>(data);
      vectors.push_back(makeRowVector({"c0"}, {c0}));
      startingValue += vectorSizes[j];
    }

    return vectors;
  }

  std::string getDuckDBQuery(
      const std::map<std::string, std::vector<int64_t>>& rowGroupSizesForFiles,
      const std::unordered_map<
          std::string,
          std::multimap<std::string, std::vector<int64_t>>>&
          deleteFilesForBaseDatafiles) {
    int64_t totalNumRowsInAllBaseFiles = 0;
    std::map<std::string, int64_t> baseFileSizes;
    for (auto rowGroupSizesInFile : rowGroupSizesForFiles) {
      // Sum up the row counts in all RowGroups in each base file
      baseFileSizes[rowGroupSizesInFile.first] += std::accumulate(
          rowGroupSizesInFile.second.begin(),
          rowGroupSizesInFile.second.end(),
          0LL);
      totalNumRowsInAllBaseFiles += baseFileSizes[rowGroupSizesInFile.first];
    }

    // Group the delete vectors by baseFileName
    std::map<std::string, std::vector<std::vector<int64_t>>>
        deletePosVectorsForAllBaseFiles;
    for (auto deleteFile : deleteFilesForBaseDatafiles) {
      auto deleteFileContent = deleteFile.second;
      for (auto rowGroup : deleteFileContent) {
        auto baseFileName = rowGroup.first;
        deletePosVectorsForAllBaseFiles[baseFileName].push_back(
            rowGroup.second);
      }
    }

    // Flatten and deduplicate the delete position vectors in
    // deletePosVectorsForAllBaseFiles from previous step, and count the total
    // number of distinct delete positions for all base files
    std::map<std::string, std::vector<int64_t>>
        flattenedDeletePosVectorsForAllBaseFiles;
    int64_t totalNumDeletePositions = 0;
    for (auto deleteVectorsForBaseFile : deletePosVectorsForAllBaseFiles) {
      auto baseFileName = deleteVectorsForBaseFile.first;
      auto deletePositionVectors = deleteVectorsForBaseFile.second;
      std::vector<int64_t> deletePositionVector =
          flattenAndDedup(deletePositionVectors, baseFileSizes[baseFileName]);
      flattenedDeletePosVectorsForAllBaseFiles[baseFileName] =
          deletePositionVector;
      totalNumDeletePositions += deletePositionVector.size();
    }

    // Now build the DuckDB queries
    if (totalNumDeletePositions == 0) {
      return "SELECT * FROM tmp";
    } else if (totalNumDeletePositions >= totalNumRowsInAllBaseFiles) {
      return "SELECT * FROM tmp WHERE 1 = 0";
    } else {
      // Convert the delete positions in all base files into column values
      std::vector<int64_t> allDeleteValues;

      int64_t numRowsInPreviousBaseFiles = 0;
      for (auto baseFileSize : baseFileSizes) {
        auto deletePositions =
            flattenedDeletePosVectorsForAllBaseFiles[baseFileSize.first];

        if (numRowsInPreviousBaseFiles > 0) {
          for (int64_t& deleteValue : deletePositions) {
            deleteValue += numRowsInPreviousBaseFiles;
          }
        }

        allDeleteValues.insert(
            allDeleteValues.end(),
            deletePositions.begin(),
            deletePositions.end());

        numRowsInPreviousBaseFiles += baseFileSize.second;
      }

      return fmt::format(
          "SELECT * FROM tmp WHERE c0 NOT IN ({})",
          makeNotInList(allDeleteValues));
    }
  }

  std::vector<int64_t> flattenAndDedup(
      const std::vector<std::vector<int64_t>>& deletePositionVectors,
      int64_t baseFileSize) {
    std::vector<int64_t> deletePositionVector;
    for (auto vec : deletePositionVectors) {
      for (auto pos : vec) {
        if (pos >= 0 && pos < baseFileSize) {
          deletePositionVector.push_back(pos);
        }
      }
    }

    std::sort(deletePositionVector.begin(), deletePositionVector.end());
    auto last =
        std::unique(deletePositionVector.begin(), deletePositionVector.end());
    deletePositionVector.erase(last, deletePositionVector.end());

    return deletePositionVector;
  }

  template <typename T>
  std::string makeNotInList(const std::vector<T>& deletePositionVector) {
    if (deletePositionVector.empty()) {
      return "";
    }

    return std::accumulate(
        deletePositionVector.begin() + 1,
        deletePositionVector.end(),
        to<std::string>(deletePositionVector[0]),
        [](const std::string& a, const T& b) {
          return a + ", " + to<std::string>(b);
        });
  }

  core::PlanNodePtr tableScanNode(RowTypePtr outputRowType) {
    return PlanBuilder(pool_.get()).tableScan(outputRowType).planNode();
  }

  template <typename T>
  std::string makePredicates(
      const std::vector<std::vector<T>>& equalityDeleteVector,
      const std::vector<int32_t>& equalityFieldIds) {
    std::string predicates("");
    int32_t numDataColumns =
        *std::max_element(equalityFieldIds.begin(), equalityFieldIds.end());

    VELOX_CHECK_GT(numDataColumns, 0);
    VELOX_CHECK_GE(numDataColumns, equalityDeleteVector.size());
    VELOX_CHECK_GT(equalityDeleteVector.size(), 0);

    auto numDeletedValues = equalityDeleteVector[0].size();

    if (numDeletedValues == 0) {
      return predicates;
    }

    // If all values for a column are deleted, just return an always-false
    // predicate
    for (auto i = 0; i < equalityDeleteVector.size(); i++) {
      auto equalityFieldId = equalityFieldIds[i];
      auto deleteValues = equalityDeleteVector[i];

      auto lastIter = std::unique(deleteValues.begin(), deleteValues.end());
      auto numDistinctValues = lastIter - deleteValues.begin();
      if constexpr (std::is_arithmetic_v<T>) {
        auto minValue = T(1);
        auto maxValue = *std::max_element(deleteValues.begin(), lastIter);
        if (maxValue - minValue + 1 == numDistinctValues &&
            maxValue == (rowCount - 1) / equalityFieldId) {
          return "1 = 0";
        }
      }
    }

    if (equalityDeleteVector.size() == 1) {
      std::string name = fmt::format("c{}", equalityFieldIds[0] - 1);
      const auto& values = equalityDeleteVector[0];
      predicates = fmt::format(
          "{} NOT IN ({})",
          name,
          makeNotInList(std::vector<T>(values.begin(), values.end())));
    } else {
      for (int i = 0; i < numDeletedValues; i++) {
        std::string oneRow("");
        for (int j = 0; j < equalityFieldIds.size(); j++) {
          std::string name = fmt::format("c{}", equalityFieldIds[j] - 1);
          std::string predicate =
              fmt::format("({} <> {})", name, equalityDeleteVector[j][i]);

          oneRow = oneRow == "" ? predicate
                                : fmt::format("({} OR {})", oneRow, predicate);
        }

        predicates = predicates == ""
            ? oneRow
            : fmt::format("{} AND {}", predicates, oneRow);
      }
    }
    return predicates;
  }

  std::shared_ptr<IcebergMetadataColumn> pathColumn_ =
      IcebergMetadataColumn::icebergDeleteFilePathColumn();
  std::shared_ptr<IcebergMetadataColumn> posColumn_ =
      IcebergMetadataColumn::icebergDeletePosColumn();

 protected:
  RowTypePtr rowType_{ROW({"c0"}, {BIGINT()})};
  dwio::common::FileFormat fileFormat_{dwio::common::FileFormat::DWRF};

  template <typename T>
  std::shared_ptr<TempFilePath> writeEqualityDeleteFile(
      const std::vector<std::vector<T>>& equalityDeleteVector) {
    std::vector<std::string> names;
    std::vector<VectorPtr> vectors;
    for (int i = 0; i < equalityDeleteVector.size(); i++) {
      names.push_back(fmt::format("c{}", i));
      vectors.push_back(makeFlatVector<T>(equalityDeleteVector[i]));
    }

    RowVectorPtr deleteFileVectors = makeRowVector(names, vectors);

    auto deleteFilePath = TempFilePath::create();
    writeToFile(deleteFilePath->getPath(), deleteFileVectors);

    return deleteFilePath;
  }

  std::vector<std::shared_ptr<TempFilePath>> writeDataFiles(
      uint64_t numRows,
      int32_t numColumns = 1,
      int32_t splitCount = 1,
      std::vector<RowVectorPtr> dataVectors = {}) {
    if (dataVectors.empty()) {
      dataVectors = makeVectors(splitCount, numRows, numColumns);
    }
    VELOX_CHECK_EQ(dataVectors.size(), splitCount);

    std::vector<std::shared_ptr<TempFilePath>> dataFilePaths;
    dataFilePaths.reserve(splitCount);
    for (auto i = 0; i < splitCount; i++) {
      dataFilePaths.emplace_back(TempFilePath::create());
      writeToFile(dataFilePaths.back()->getPath(), dataVectors[i]);
    }

    createDuckDbTable(dataVectors);
    return dataFilePaths;
  }

  std::vector<RowVectorPtr>
  makeVectors(int32_t count, int32_t rowsPerVector, int32_t numColumns = 1) {
    std::vector<TypePtr> types(numColumns, BIGINT());
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
        auto data = makeSequenceValues(rowsPerVector, j + 1);
        vectors.push_back(vectorMaker_.flatVector<int64_t>(data));
      }

      rowVectors.push_back(makeRowVector(names, vectors));
    }

    rowType_ = std::make_shared<RowType>(std::move(names), std::move(types));

    return rowVectors;
  }
};

/// This test creates one single data file and one delete file. The parameter
/// passed to assertSingleBaseFileSingleDeleteFile is the delete positions.
TEST_F(HiveIcebergTest, singleBaseFileSinglePositionalDeleteFile) {
  folly::SingletonVault::singleton()->registrationComplete();

  assertSingleBaseFileSingleDeleteFile({{0, 1, 2, 3}});
  // Delete the first and last row in each batch (10000 rows per batch)
  assertSingleBaseFileSingleDeleteFile({{0, 9999, 10000, 19999}});
  // Delete several rows in the second batch (10000 rows per batch)
  assertSingleBaseFileSingleDeleteFile({{10000, 10002, 19999}});
  // Delete random rows
  assertSingleBaseFileSingleDeleteFile({makeRandomIncreasingValues(0, 20000)});
  // Delete 0 rows
  assertSingleBaseFileSingleDeleteFile({});
  // Delete all rows
  assertSingleBaseFileSingleDeleteFile(
      {makeContinuousIncreasingValues(0, 20000)});
  // Delete rows that don't exist
  assertSingleBaseFileSingleDeleteFile({{20000, 29999}});
}

/// This test creates 3 base data files, only the middle one has corresponding
/// delete positions. The parameter passed to
/// assertSingleBaseFileSingleDeleteFile is the delete positions.for the middle
/// base file.
TEST_F(HiveIcebergTest, MultipleBaseFilesSinglePositionalDeleteFile) {
  folly::SingletonVault::singleton()->registrationComplete();

  assertMultipleBaseFileSingleDeleteFile({0, 1, 2, 3});
  assertMultipleBaseFileSingleDeleteFile({0, 9999, 10000, 19999});
  assertMultipleBaseFileSingleDeleteFile({10000, 10002, 19999});
  assertMultipleBaseFileSingleDeleteFile({10000, 10002, 19999});
  assertMultipleBaseFileSingleDeleteFile(
      makeRandomIncreasingValues(0, rowCount));
  assertMultipleBaseFileSingleDeleteFile({});
  assertMultipleBaseFileSingleDeleteFile(
      makeContinuousIncreasingValues(0, rowCount));
}

/// This test creates one base data file/split with multiple delete files. The
/// parameter passed to assertSingleBaseFileMultipleDeleteFiles is the vector of
/// delete files. Each leaf vector represents the delete positions in that
/// delete file.
TEST_F(HiveIcebergTest, singleBaseFileMultiplePositionalDeleteFiles) {
  folly::SingletonVault::singleton()->registrationComplete();

  // Delete row 0, 1, 2, 3 from the first batch out of two.
  assertSingleBaseFileMultipleDeleteFiles({{1}, {2}, {3}, {4}});
  // Delete the first and last row in each batch (10000 rows per batch).
  assertSingleBaseFileMultipleDeleteFiles({{0}, {9999}, {10000}, {19999}});

  assertSingleBaseFileMultipleDeleteFiles({{500, 21000}});

  assertSingleBaseFileMultipleDeleteFiles(
      {makeRandomIncreasingValues(0, 10000),
       makeRandomIncreasingValues(10000, 20000),
       makeRandomIncreasingValues(5000, 15000)});

  assertSingleBaseFileMultipleDeleteFiles(
      {makeContinuousIncreasingValues(0, 10000),
       makeContinuousIncreasingValues(10000, 20000)});

  assertSingleBaseFileMultipleDeleteFiles(
      {makeContinuousIncreasingValues(0, 10000),
       makeContinuousIncreasingValues(10000, 20000),
       makeRandomIncreasingValues(5000, 15000)});

  assertSingleBaseFileMultipleDeleteFiles(
      {makeContinuousIncreasingValues(0, 20000),
       makeContinuousIncreasingValues(0, 20000)});

  assertSingleBaseFileMultipleDeleteFiles(
      {makeRandomIncreasingValues(0, 20000),
       {},
       makeRandomIncreasingValues(5000, 15000)});

  assertSingleBaseFileMultipleDeleteFiles({{}, {}});
}

/// This test creates 2 base data files, and 1 or 2 delete files, with unaligned
/// RowGroup boundaries
TEST_F(HiveIcebergTest, multipleBaseFileMultiplePositionalDeleteFiles) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::map<std::string, std::vector<int64_t>> rowGroupSizesForFiles;
  std::unordered_map<
      std::string,
      std::multimap<std::string, std::vector<int64_t>>>
      deleteFilesForBaseDatafiles;

  // Create two data files, each with two RowGroups
  rowGroupSizesForFiles["data_file_1"] = {100, 85};
  rowGroupSizesForFiles["data_file_2"] = {99, 1};

  // Delete 3 rows from the first RowGroup in data_file_1
  deleteFilesForBaseDatafiles["delete_file_1"] = {{"data_file_1", {0, 1, 99}}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // Delete 3 rows from the second RowGroup in data_file_1
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", {100, 101, 184}}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // Delete random rows from the both RowGroups in data_file_1
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", makeRandomIncreasingValues(0, 185)}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // Delete all rows in data_file_1
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", makeContinuousIncreasingValues(0, 185)}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);
  //
  // Delete non-existent rows from data_file_1
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", makeRandomIncreasingValues(186, 300)}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // Delete several rows from both RowGroups in both data files
  deleteFilesForBaseDatafiles.clear();
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", {0, 100, 102, 184}}, {"data_file_2", {1, 98, 99}}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // The delete file delete_file_1 contains 3 RowGroups itself, with the first 3
  // deleting some repeating rows in data_file_1, and the last 2 RowGroups
  // deleting some  repeating rows in data_file_2
  deleteFilesForBaseDatafiles.clear();
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", {0, 1, 2, 3}},
      {"data_file_1", {1, 2, 3, 4}},
      {"data_file_1", makeRandomIncreasingValues(0, 185)},
      {"data_file_2", {1, 3, 5, 7}},
      {"data_file_2", makeRandomIncreasingValues(0, 100)}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // delete_file_2 contains non-overlapping delete rows for each data files in
  // each RowGroup
  deleteFilesForBaseDatafiles.clear();
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", {0, 1, 2, 3}}, {"data_file_2", {1, 3, 5, 7}}};
  deleteFilesForBaseDatafiles["delete_file_2"] = {
      {"data_file_1", {1, 2, 3, 4}},
      {"data_file_1", {98, 99, 100, 101, 184}},
      {"data_file_2", {3, 5, 7, 9}},
      {"data_file_2", {98, 99, 100}}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // Two delete files each containing overlapping delete rows for both data
  // files
  deleteFilesForBaseDatafiles.clear();
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", makeRandomIncreasingValues(0, 185)},
      {"data_file_2", makeRandomIncreasingValues(0, 100)}};
  deleteFilesForBaseDatafiles["delete_file_2"] = {
      {"data_file_1", makeRandomIncreasingValues(10, 120)},
      {"data_file_2", makeRandomIncreasingValues(50, 100)}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);
}

TEST_F(HiveIcebergTest, positionalDeletesMultipleSplits) {
  folly::SingletonVault::singleton()->registrationComplete();

  assertMultipleSplits({1, 2, 3, 4}, 10, 5);
  assertMultipleSplits({1, 2, 3, 4}, 10, 0);
  assertMultipleSplits({1, 2, 3, 4}, 10, 10);
  assertMultipleSplits({0, 9999, 10000, 19999}, 10, 3);
  assertMultipleSplits(makeRandomIncreasingValues(0, 20000), 10, 3);
  assertMultipleSplits(makeContinuousIncreasingValues(0, 20000), 10, 3);
  assertMultipleSplits({}, 10, 3);

  assertMultipleSplits({1, 2, 3, 4}, 10, 5, 30000, 3);
  assertPositionalDeletes(
      {
          {"data_file_0", {500}},
          {"data_file_1", {10000, 10000}},
          {"data_file_2", {500}},
      },
      {{"delete_file_1",
        {{"data_file_1", makeRandomIncreasingValues(0, 20000)}}}},
      0,
      3);

  // Include only upper bound(which is exclusive) in delete positions for the
  // second 10k batch of rows.
  assertMultipleSplits({1000, 9000, 20000}, 1, 0, 20000, 3);
}

TEST_F(HiveIcebergTest, testPartitionedRead) {
  RowTypePtr rowType{ROW({"c0", "ds"}, {BIGINT(), DateType::get()})};
  std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
  // Iceberg API sets partition values for dates to daysSinceEpoch, so
  // in velox, we do not need to convert it to days.
  // Test query on two partitions ds=17627(2018-04-06), ds=17628(2018-04-07)
  std::vector<std::shared_ptr<ConnectorSplit>> splits;
  std::vector<std::shared_ptr<TempFilePath>> dataFilePaths;
  for (int i = 0; i <= 1; ++i) {
    std::vector<RowVectorPtr> dataVectors;
    int32_t daysSinceEpoch = 17627 + i;
    VectorPtr c0 = makeFlatVector<int64_t>((std::vector<int64_t>){i});
    VectorPtr ds =
        makeFlatVector<int32_t>((std::vector<int32_t>){daysSinceEpoch});
    dataVectors.push_back(makeRowVector({"c0", "ds"}, {c0, ds}));

    auto dataFilePath = TempFilePath::create();
    dataFilePaths.push_back(dataFilePath);
    writeToFile(
        dataFilePath->getPath(), dataVectors, config_, flushPolicyFactory_);
    partitionKeys["ds"] = std::to_string(daysSinceEpoch);
    auto icebergSplits =
        makeIcebergSplits(dataFilePath->getPath(), {}, partitionKeys);
    splits.insert(splits.end(), icebergSplits.begin(), icebergSplits.end());
  }

  connector::ColumnHandleMap assignments;
  assignments.insert(
      {"c0",
       std::make_shared<HiveColumnHandle>(
           "c0",
           HiveColumnHandle::ColumnType::kRegular,
           rowType->childAt(0),
           rowType->childAt(0))});

  std::vector<common::Subfield> requiredSubFields;
  HiveColumnHandle::ColumnParseParameters columnParseParameters;
  columnParseParameters.partitionDateValueFormat =
      HiveColumnHandle::ColumnParseParameters::kDaysSinceEpoch;
  assignments.insert(
      {"ds",
       std::make_shared<HiveColumnHandle>(
           "ds",
           HiveColumnHandle::ColumnType::kPartitionKey,
           rowType->childAt(1),
           rowType->childAt(1),
           std::move(requiredSubFields),
           columnParseParameters)});

  auto plan = PlanBuilder(pool_.get())
                  .tableScan(rowType, {}, "", nullptr, assignments)
                  .planNode();

  HiveConnectorTestBase::assertQuery(
      plan,
      splits,
      "SELECT * FROM (VALUES (0, '2018-04-06'), (1, '2018-04-07'))",
      0);

  // Test filter on non-partitioned non-date column
  std::vector<std::string> nonPartitionFilters = {"c0 = 1"};
  plan = PlanBuilder(pool_.get())
             .tableScan(rowType, nonPartitionFilters, "", nullptr, assignments)
             .planNode();

  HiveConnectorTestBase::assertQuery(plan, splits, "SELECT 1, '2018-04-07'");

  // Test filter on non-partitioned date column
  std::vector<std::string> filters = {"ds = date'2018-04-06'"};
  plan = PlanBuilder(pool_.get()).tableScan(rowType, filters).planNode();

  splits.clear();
  for (auto& dataFilePath : dataFilePaths) {
    auto icebergSplits = makeIcebergSplits(dataFilePath->getPath());
    splits.insert(splits.end(), icebergSplits.begin(), icebergSplits.end());
  }

  HiveConnectorTestBase::assertQuery(plan, splits, "SELECT 0, '2018-04-06'");
}

// Delete values from a single column file
TEST_F(HiveIcebergTest, equalityDeletesSingleFileColumn1) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<int64_t>>>
      equalityDeleteVectorMap;
  equalityFieldIdsMap.insert({0, {1}});

  // Delete row 0, 1, 2, 3 from the first batch out of two.
  equalityDeleteVectorMap.insert({0, {{0, 1, 2, 3}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete the first and last row in each batch (10000 rows per batch)
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{0, 9999, 10000, 19999}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete several rows in the second batch (10000 rows per batch)
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{10000, 10002, 19999}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete random rows
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {makeRandomDeleteValues(rowCount)}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete 0 rows
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete all rows
  equalityDeleteVectorMap.insert({0, {makeSequenceValues(rowCount)}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete rows that don't exist
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{20000, 29999}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);
}

// Delete values from the second column in a 2-column file
//
//    c1    c2
//    0     0
//    1     0
//    2     1
//    3     1
//    4     2
//  ...    ...
//  19999 9999
TEST_F(HiveIcebergTest, equalityDeletesSingleFileColumn2) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<int64_t>>>
      equalityDeleteVectorMap;
  equalityFieldIdsMap.insert({0, {2}});

  // Delete values 0, 1, 2, 3 from the second column
  equalityDeleteVectorMap.insert({0, {{0, 1, 2, 3}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete the smallest value 0 and the largest value 9999 from the second
  // column, which has the range [0, 9999]
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{0, 9999}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete non-existent values from the second column
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{10000, 10002, 19999}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete random rows from the second column
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {makeSequenceValues(rowCount)}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  //     Delete 0 values
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete all values
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {makeSequenceValues(rowCount / 2)}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);
}

// Delete values from 2 columns with the following data:
//
//    c1    c2
//    0     0
//    1     0
//    2     1
//    3     1
//    4     2
//  ...    ...
//  19999 9999
TEST_F(HiveIcebergTest, equalityDeletesSingleFileMultipleColumns) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<int64_t>>>
      equalityDeleteVectorMap;
  equalityFieldIdsMap.insert({0, {1, 2}});

  // Delete rows 0, 1
  equalityDeleteVectorMap.insert({0, {{0, 1}, {0, 0}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete rows 0, 2, 4, 6
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{0, 2, 4, 6}, {0, 1, 2, 3}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  //   Delete the last row
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{19999}, {9999}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete non-existent values
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{20000, 30000}, {10000, 1500}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete 0 values
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{}, {}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete all values
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert(
      {0, {makeSequenceValues(rowCount), makeSequenceValues(rowCount, 2)}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE 1 = 0");
}

TEST_F(HiveIcebergTest, equalityDeletesMultipleFiles) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<int64_t>>>
      equalityDeleteVectorMap;
  equalityFieldIdsMap.insert({{0, {1}}, {1, {2}}});

  // Delete rows {0, 1} from c0, {2, 3} from c1, with two equality delete files
  equalityDeleteVectorMap.insert({{0, {{0, 1}}}, {1, {{2, 3}}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete using 3 equality delete files
  equalityFieldIdsMap.insert({{2, {3}}});
  equalityDeleteVectorMap.insert({{0, {{0, 1}}}, {1, {{2, 3}}}, {2, {{4, 5}}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete 0 values
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({{0, {{}}}, {1, {{}}}, {2, {{}}}});
  assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

  // Delete all values
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert(
      {{0, {makeSequenceValues(rowCount)}},
       {1, {makeSequenceValues(rowCount)}},
       {2, {makeSequenceValues(rowCount)}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE 1 = 0");
}

TEST_F(HiveIcebergTest, equalityDeletesFloatAndDoubleThrowsError) {
  folly::SingletonVault::singleton()->registrationComplete();

  // Test for float (REAL)
  {
    std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
    std::unordered_map<int8_t, std::vector<std::vector<float>>>
        equalityDeleteVectorMap;
    equalityFieldIdsMap.insert({{0, {1}}, {1, {2}}});
    equalityDeleteVectorMap.insert({{0, {{0, 1}}}, {1, {{2, 3}}}});
    VELOX_ASSERT_THROW(
        assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap),
        "Iceberg does not allow DOUBLE or REAL columns as the equality delete columns: c1 : REAL");
  }

  // Test for double (DOUBLE)
  {
    std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
    std::unordered_map<int8_t, std::vector<std::vector<double>>>
        equalityDeleteVectorMap;
    equalityFieldIdsMap.insert({{0, {1}}, {1, {2}}});
    equalityDeleteVectorMap.insert({{0, {{0, 1}}}, {1, {{2, 3}}}});
    VELOX_ASSERT_THROW(
        assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap),
        "Iceberg does not allow DOUBLE or REAL columns as the equality delete columns: c1 : DOUBLE");
  }
}

TEST_F(HiveIcebergTest, TestSubFieldEqualityDelete) {
  folly::SingletonVault::singleton()->registrationComplete();

  // Write the base file
  std::shared_ptr<TempFilePath> dataFilePath = TempFilePath::create();
  std::vector<RowVectorPtr> dataVectors = {makeRowVector(
      {"c_bigint", "c_row"},
      {makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
       makeRowVector(
           {"c0", "c1", "c2"},
           {makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
            makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
            makeFlatVector<int64_t>(20, [](auto row) { return row + 1; })})})};
  int32_t numDataColumns = 1;
  dataFilePath = writeDataFiles(rowCount, numDataColumns, 1, dataVectors)[0];

  // Write the delete file. Equality delete field is c_row.c1
  std::vector<IcebergDeleteFile> deleteFiles;
  // Delete rows {0, 1} from c_row.c1, whose schema Id is 4
  std::vector<RowVectorPtr> deleteDataVectors = {makeRowVector(
      {"c1"}, {makeFlatVector<int64_t>(2, [](auto row) { return row + 1; })})};

  std::vector<std::shared_ptr<TempFilePath>> deleteFilePaths;
  auto equalityFieldIds = std::vector<int32_t>({4});
  auto deleteFilePath = TempFilePath::create();
  writeToFile(deleteFilePath->getPath(), deleteDataVectors.back());
  deleteFilePaths.push_back(deleteFilePath);
  IcebergDeleteFile deleteFile(
      FileContent::kEqualityDeletes,
      deleteFilePaths.back()->getPath(),
      fileFormat_,
      2,
      testing::internal::GetFileSize(
          std::fopen(deleteFilePaths.back()->getPath().c_str(), "r")),
      equalityFieldIds);
  deleteFiles.push_back(deleteFile);

  auto icebergSplits = makeIcebergSplits(dataFilePath->getPath(), deleteFiles);

  // Select both c_bigint and c_row column columns
  std::string duckDbSql = "SELECT * FROM tmp WHERE c_row.c0 not in (1, 2)";
  assertEqualityDeletes(
      icebergSplits.back(), asRowType(dataVectors[0]->type()), duckDbSql);

  // SELECT only c_bigint column
  duckDbSql = "SELECT c_bigint FROM tmp WHERE c_row.c0 not in (1, 2)";
  assertEqualityDeletes(
      icebergSplits.back(), ROW({"c_bigint"}, {BIGINT()}), duckDbSql);
}

TEST_F(HiveIcebergTest, equalityDeletesWithNegatedVarbinaryValues) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<std::string>>>
      equalityDeleteVectorMap;

  equalityFieldIdsMap.insert({0, {1}});
  equalityDeleteVectorMap.insert({0, {{"\x01\x02", "\x05\x06"}}});

  std::vector<RowVectorPtr> dataVectors = {makeRowVector(
      {"c0"},
      {makeFlatVector<std::string_view>(
          {"\x01\x02", "\x03\x04", "\x05\x06", "\x07\x08", "\x09\x0A"})})};

  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE hex(c0) NOT IN ('0102', '0506')",
      dataVectors);
}

TEST_F(HiveIcebergTest, equalityDeletesWithNegatedVarbinaryStringValues) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<std::string>>>
      equalityDeleteVectorMap;

  equalityFieldIdsMap.insert({0, {1}});
  equalityDeleteVectorMap.insert({0, {{"apple", "cherry"}}});

  std::vector<RowVectorPtr> dataVectors = {makeRowVector(
      {"c0"},
      {makeFlatVector<std::string>(
          {"apple", "banana", "cherry", "date", "elderberry"})})};

  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE c0 NOT IN ('apple', 'cherry')",
      dataVectors);
}

TEST_F(HiveIcebergTest, equalityDeletesStringSingleFileColumn1) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<std::string>>>
      equalityDeleteVectorMap;
  equalityFieldIdsMap.insert({0, {1}});

  // Delete "apple", "banana"
  equalityDeleteVectorMap.insert({0, {{"apple", "banana"}}});
  std::vector<RowVectorPtr> dataVectors = {makeRowVector(
      {"c0"},
      {makeFlatVector<std::string>(
          {"apple", "banana", "cherry", "date", "elderberry"})})};
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE c0 NOT IN ('apple', 'banana')",
      dataVectors);

  // Delete first and last
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{"apple", "elderberry"}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE c0 NOT IN ('apple', 'elderberry')",
      dataVectors);

  // Delete non-existent
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{"fig", "grape"}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE c0 NOT IN ('fig', 'grape')",
      dataVectors);

  // Delete all
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert(
      {0, {{"apple", "banana", "cherry", "date", "elderberry"}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE 1 = 0",
      dataVectors);

  // Delete none
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp",
      dataVectors);
}

TEST_F(HiveIcebergTest, equalityDeletesVarbinarySingleFileColumn1) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<std::string>>>
      equalityDeleteVectorMap;
  equalityFieldIdsMap.insert({0, {1}});

  // Delete "\x01\x02", "\x03\x04"
  equalityDeleteVectorMap.insert({0, {{"\x01\x02", "\x03\x04"}}});
  std::vector<RowVectorPtr> dataVectors = {makeRowVector(
      {"c0"},
      {makeFlatVector<std::string_view>(
          {"\x01\x02", "\x03\x04", "\x05\x06", "\x07\x08", "\x09\x0A"})})};
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE hex(c0) NOT IN ('0102', '0304')",
      dataVectors);

  // Delete all
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert(
      {0, {{"\x01\x02", "\x03\x04", "\x05\x06", "\x07\x08", "\x09\x0A"}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE 1 = 0",
      dataVectors);

  // Delete none
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp",
      dataVectors);
}

TEST_F(HiveIcebergTest, equalityDeletesStringSingleFileMultipleColumns) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<std::string>>>
      equalityDeleteVectorMap;
  equalityFieldIdsMap.insert({0, {1, 2}});

  // Delete ("apple", "banana"), ("cherry", "date")
  equalityDeleteVectorMap.insert(
      {0, {{"apple", "cherry"}, {"banana", "date"}}});
  std::vector<RowVectorPtr> dataVectors = {makeRowVector(
      {"c0", "c1"},
      {makeFlatVector<std::string>({"apple", "cherry", "elderberry", "fig"}),
       makeFlatVector<std::string>({"banana", "date", "grape", "honeydew"})})};
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE ((c0 <> 'apple' OR c1 <> 'banana') AND (c0 <> 'cherry' OR c1 <> 'date'))",
      dataVectors);

  // Delete none
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{}, {}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp",
      dataVectors);

  // Delete all
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert(
      {0,
       {{"apple", "cherry", "elderberry", "fig"},
        {"banana", "date", "grape", "honeydew"}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE 1 = 0",
      dataVectors);
}

TEST_F(HiveIcebergTest, equalityDeletesVarbinarySingleFileMultipleColumns) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<std::string>>>
      equalityDeleteVectorMap;
  equalityFieldIdsMap.insert({0, {1, 2}});

  // Delete (b"\x01\x02", b"\x05\x06"), (b"\x03\x04", b"\x07\x08")
  equalityDeleteVectorMap.insert(
      {0, {{"\x01\x02", "\x03\x04"}, {"\x05\x06", "\x07\x08"}}});
  std::vector<RowVectorPtr> dataVectors = {makeRowVector(
      {"c0", "c1"},
      {makeFlatVector<std::string_view>({"\x01\x02", "\x03\x04", "\x09\x0A"}),
       makeFlatVector<std::string_view>(
           {"\x05\x06", "\x07\x08", "\x0B\x0C"})})};
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE ((hex(c0) <> '0102' OR hex(c1) <> '0506') AND (hex(c0) <> '0304' OR hex(c1) <> '0708'))",
      dataVectors);

  // Delete none
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{}, {}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp",
      dataVectors);

  // Delete all
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert(
      {0,
       {{"\x01\x02", "\x03\x04", "\x09\x0A"},
        {"\x05\x06", "\x07\x08", "\x0B\x0C"}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE 1 = 0",
      dataVectors);
}

TEST_F(HiveIcebergTest, equalityDeleteFileWithIntAndVarcharColumns) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<std::string>>>
      equalityDeleteVectorMap;

  equalityFieldIdsMap.insert({0, {1, 2}});

  // Delete rows with (int: 2, varchar: "banana") and (int: 4, varchar: "date")
  equalityDeleteVectorMap.insert({0, {{"2", "4"}, {"banana", "date"}}});
  std::vector<RowVectorPtr> dataVectors = {makeRowVector(
      {"c0", "c1"},
      {makeFlatVector<std::string>({"1", "2", "3", "4", "5"}),
       makeFlatVector<std::string>(
           {"apple", "banana", "cherry", "date", "elderberry"})})};

  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE ((c0 <> '2' OR c1 <> 'banana') AND (c0 <> '4' OR c1 <> 'date'))",
      dataVectors);

  // Delete none
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{}, {}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp",
      dataVectors);

  // Delete all rows
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert(
      {0,
       {{"1", "2", "3", "4", "5"},
        {"apple", "banana", "cherry", "date", "elderberry"}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE 1 = 0",
      dataVectors);
}

TEST_F(HiveIcebergTest, equalityDeleteFileWithIntAndVarbinaryColumns) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<std::string>>>
      equalityDeleteVectorMap;

  equalityFieldIdsMap.insert({0, {1, 2}});

  // Delete rows with (int: 2, varbinary: "\x03\x04") and (int: 4, varbinary:
  // "\x07\x08")
  equalityDeleteVectorMap.insert({0, {{"2", "4"}, {"\x03\x04", "\x07\x08"}}});
  std::vector<RowVectorPtr> dataVectors = {makeRowVector(
      {"c0", "c1"},
      {makeFlatVector<std::string>({"1", "2", "3", "4", "5"}),
       makeFlatVector<std::string_view>(
           {"\x01\x02", "\x03\x04", "\x05\x06", "\x07\x08", "\x09\x0A"})})};

  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE ((c0 <> '2' OR hex(c1) <> '0304') AND (c0 <> '4' OR hex(c1) <> '0708'))",
      dataVectors);

  // Delete none
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{}, {}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp",
      dataVectors);

  // Delete all rows
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert(
      {0,
       {{"1", "2", "3", "4", "5"},
        {"\x01\x02", "\x03\x04", "\x05\x06", "\x07\x08", "\x09\x0A"}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE 1 = 0",
      dataVectors);
}

TEST_F(HiveIcebergTest, equalityDeletesShortDecimal) {
  folly::SingletonVault::singleton()->registrationComplete();

  // Use DECIMAL(6, 2) for short decimal (precision 6, scale 2)
  auto decimalType = DECIMAL(6, 2);
  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<int64_t>>>
      equalityDeleteVectorMap;
  equalityFieldIdsMap.insert({0, {1}});

  // Values: 123456 (represents 1234.56), 789012 (represents 7890.12)
  equalityDeleteVectorMap.insert({0, {{123456, 789012}}});
  std::vector<RowVectorPtr> dataVectors = {makeRowVector(
      {"c0"},
      {makeFlatVector<int64_t>(
          {123456, 789012, 345678, 901234, 567890}, decimalType)})};

  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE c0 NOT IN (1234.56, 7890.12)",
      dataVectors);

  // Delete all
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert(
      {0, {{123456, 789012, 345678, 901234, 567890}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp WHERE 1 = 0",
      dataVectors);

  // Delete none
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{}}});
  assertEqualityDeletes(
      equalityDeleteVectorMap,
      equalityFieldIdsMap,
      "SELECT * FROM tmp",
      dataVectors);
}

TEST_F(HiveIcebergTest, equalityDeletesLongDecimal) {
  folly::SingletonVault::singleton()->registrationComplete();

  // Use DECIMAL(25, 5) for long decimal (precision 25, scale 5)
  auto decimalType = DECIMAL(25, 5);
  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<int128_t>>>
      equalityDeleteVectorMap;
  equalityFieldIdsMap.insert({0, {1}});

  // Values: 123456789012345 (represents 1234567.89012), 987654321098765
  // (represents 9876543.21098)
  equalityDeleteVectorMap.insert(
      {0, {{int128_t(123456789012345), int128_t(987654321098765)}}});
  std::vector<RowVectorPtr> dataVectors = {makeRowVector(
      {"c0"},
      {makeFlatVector<int128_t>(
          {(123456789012345),
           (987654321098765),
           (111111111111111),
           (222222222222222),
           (333333333333333)},
          decimalType)})};

  VELOX_ASSERT_THROW(
      assertEqualityDeletes(
          equalityDeleteVectorMap,
          equalityFieldIdsMap,
          "SELECT * FROM tmp WHERE c0 NOT IN (123456789012345, 987654321098765)",
          dataVectors),
      "Decimal is not supported for DWRF.");
}

// Refactored Equality Deletes Tests with Comprehensive Type Support

// Replace the custom type structs with proper Velox type usage
// Remove all the custom type tag structs (Int8Type, Int16Type, etc.) and TestDataGenerator specializations

// Add this helper class for type operations
class TypeTestHelper {
public:
  template<typename T>
  static TypePtr getVeloxType() {
    if constexpr (std::is_same_v<T, int8_t>) {
      return TINYINT();
    } else if constexpr (std::is_same_v<T, int16_t>) {
      return SMALLINT();
    } else if constexpr (std::is_same_v<T, int32_t>) {
      return INTEGER();
    } else if constexpr (std::is_same_v<T, int64_t>) {
      return BIGINT();
    } else if constexpr (std::is_same_v<T, float>) {
      return REAL();
    } else if constexpr (std::is_same_v<T, double>) {
      return DOUBLE();
    } else if constexpr (std::is_same_v<T, std::string>) {
      return VARCHAR();
    } else if constexpr (std::is_same_v<T, int128_t>) {
      return DECIMAL(25, 5); // Long decimal
    } else {
      return VARCHAR(); // Default fallback
    }
  }

  template<typename T>
  static std::vector<T> generateTestData(size_t count) {
    std::vector<T> data;
    if constexpr (std::is_arithmetic_v<T>) {
      data.resize(count);
      std::iota(data.begin(), data.end(), T(0));
    } else if constexpr (std::is_same_v<T, std::string>) {
      std::vector<std::string> fruits = {"apple", "banana", "cherry", "date", "elderberry"};
      for (size_t i = 0; i < count && i < fruits.size(); ++i) {
        data.push_back(fruits[i]);
      }
    }
    return data;
  }

  template<typename T>
  static std::string formatValueForSql(const T& value) {
    if constexpr (std::is_same_v<T, std::string>) {
      return "'" + value + "'";
    } else {
      return std::to_string(value);
    }
  }
};

// Replace the HiveIcebergEqualityDeletesTest class methods with corrected versions:

class HiveIcebergEqualityDeletesTest : public HiveIcebergTest {
public:
  enum class TestScenario {
    DELETE_SUBSET,
    DELETE_FIRST_LAST,
    DELETE_ALL,
    DELETE_NONE,
    DELETE_NONEXISTENT
  };

  // Simplified single column test for basic types
  template<typename T>
  void testSingleColumnEqualityDeletes(TestScenario scenario, int32_t fieldId = 1) {
    auto dataValues = TypeTestHelper::generateTestData<T>(5);
    std::vector<T> deleteValues;
    std::string expectedSql;

    switch (scenario) {
      case TestScenario::DELETE_SUBSET:
        deleteValues = {dataValues[0], dataValues[1]};
        expectedSql = buildSingleColumnSql(deleteValues, fieldId, false);
        break;
      case TestScenario::DELETE_ALL:
        deleteValues = dataValues;
        expectedSql = "SELECT * FROM tmp WHERE 1 = 0";
        break;
      case TestScenario::DELETE_NONE:
        deleteValues = {};
        expectedSql = "SELECT * FROM tmp";
        break;
      default:
        deleteValues = {dataValues[0]};
        expectedSql = buildSingleColumnSql(deleteValues, fieldId, false);
        break;
    }

    executeSingleColumnTest<T>(fieldId, deleteValues, dataValues, expectedSql);
  }

  // Test error-throwing types
  template<typename T>
  void testErrorThrowingType() {
    auto dataValues = TypeTestHelper::generateTestData<T>(3);
    std::vector<T> deleteValues = {dataValues[0], dataValues[1]};

    std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
    equalityFieldIdsMap.insert({0, {1}});

    std::unordered_map<int8_t, std::vector<std::vector<T>>> equalityDeleteVectorMap;
    equalityDeleteVectorMap.insert({0, {deleteValues}});

    std::vector<RowVectorPtr> dataVectors = createDataVectors<T>(dataValues);

    if constexpr (std::is_same_v<T, float>) {
      VELOX_ASSERT_THROW(
          assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap, "", dataVectors),
          "Iceberg does not allow DOUBLE or REAL columns as the equality delete columns: c0 : REAL");
    } else if constexpr (std::is_same_v<T, double>) {
      VELOX_ASSERT_THROW(
          assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap, "", dataVectors),
          "Iceberg does not allow DOUBLE or REAL columns as the equality delete columns: c0 : DOUBLE");
    } else if constexpr (std::is_same_v<T, int128_t>) {
      VELOX_ASSERT_THROW(
          assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap, "", dataVectors),
          "Decimal is not supported for DWRF.");
    }
  }

  // Multi-column test with proper type handling
  template<typename T1, typename T2>
  void testMultiColumnEqualityDeletes(TestScenario scenario, int32_t fieldId1 = 1, int32_t fieldId2 = 2) {
    auto dataValues1 = TypeTestHelper::generateTestData<T1>(4);
    auto dataValues2 = TypeTestHelper::generateTestData<T2>(4);

    std::vector<T1> deleteValues1;
    std::vector<T2> deleteValues2;
    std::string expectedSql;

    switch (scenario) {
      case TestScenario::DELETE_SUBSET:
        deleteValues1 = {dataValues1[0], dataValues1[1]};
        deleteValues2 = {dataValues2[0], dataValues2[1]};
        expectedSql = buildMultiColumnSql<T1, T2>(deleteValues1, deleteValues2, fieldId1, fieldId2);
        break;
      case TestScenario::DELETE_ALL:
        deleteValues1 = dataValues1;
        deleteValues2 = dataValues2;
        expectedSql = "SELECT * FROM tmp WHERE 1 = 0";
        break;
      case TestScenario::DELETE_NONE:
        deleteValues1 = {};
        deleteValues2 = {};
        expectedSql = "SELECT * FROM tmp";
        break;
      default:
        deleteValues1 = {dataValues1[0]};
        deleteValues2 = {dataValues2[0]};
        expectedSql = buildMultiColumnSql<T1, T2>(deleteValues1, deleteValues2, fieldId1, fieldId2);
        break;
    }

    executeMultiColumnTest<T1, T2>(fieldId1, fieldId2, deleteValues1, deleteValues2,
                                   dataValues1, dataValues2, expectedSql);
  }

private:
  template<typename T>
  void executeSingleColumnTest(
      int32_t fieldId,
      const std::vector<T>& deleteValues,
      const std::vector<T>& dataValues,
      const std::string& expectedSql) {

    std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
    equalityFieldIdsMap.insert({0, {fieldId}});

    std::unordered_map<int8_t, std::vector<std::vector<T>>> equalityDeleteVectorMap;
    equalityDeleteVectorMap.insert({0, {deleteValues}});

    std::vector<RowVectorPtr> dataVectors = createDataVectors<T>(dataValues);

    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap, expectedSql, dataVectors);
  }

  template<typename T1, typename T2>
  void executeMultiColumnTest(
      int32_t fieldId1, int32_t fieldId2,
      const std::vector<T1>& deleteValues1, const std::vector<T2>& deleteValues2,
      const std::vector<T1>& dataValues1, const std::vector<T2>& dataValues2,
      const std::string& expectedSql) {

    std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
    equalityFieldIdsMap.insert({0, {fieldId1, fieldId2}});

    // Convert to string vectors as expected by the test framework
    std::unordered_map<int8_t, std::vector<std::vector<std::string>>> equalityDeleteVectorMap;
    std::vector<std::string> strDeleteValues1, strDeleteValues2;

    for (const auto& val : deleteValues1) {
      if constexpr (std::is_same_v<T1, std::string>) {
        strDeleteValues1.push_back(val);
      } else {
        strDeleteValues1.push_back(std::to_string(val));
      }
    }
    for (const auto& val : deleteValues2) {
      if constexpr (std::is_same_v<T2, std::string>) {
        strDeleteValues2.push_back(val);
      } else {
        strDeleteValues2.push_back(std::to_string(val));
      }
    }

    equalityDeleteVectorMap.insert({0, {strDeleteValues1, strDeleteValues2}});

    std::vector<RowVectorPtr> dataVectors = createMultiColumnDataVectors<T1, T2>(dataValues1, dataValues2);

    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap, expectedSql, dataVectors);
  }

  template<typename T>
  std::vector<RowVectorPtr> createDataVectors(const std::vector<T>& values) {
    auto type = TypeTestHelper::getVeloxType<T>();

    if constexpr (std::is_same_v<T, int8_t>) {
      return {makeRowVector({"c0"}, {makeFlatVector<int8_t>(values)})};
    } else if constexpr (std::is_same_v<T, int16_t>) {
      return {makeRowVector({"c0"}, {makeFlatVector<int16_t>(values)})};
    } else if constexpr (std::is_same_v<T, int32_t>) {
      return {makeRowVector({"c0"}, {makeFlatVector<int32_t>(values)})};
    } else if constexpr (std::is_same_v<T, int64_t>) {
      return {makeRowVector({"c0"}, {makeFlatVector<int64_t>(values)})};
    } else if constexpr (std::is_same_v<T, float>) {
      return {makeRowVector({"c0"}, {makeFlatVector<float>(values)})};
    } else if constexpr (std::is_same_v<T, double>) {
      return {makeRowVector({"c0"}, {makeFlatVector<double>(values)})};
    } else if constexpr (std::is_same_v<T, std::string>) {
      return {makeRowVector({"c0"}, {makeFlatVector<std::string>(values)})};
    } else if constexpr (std::is_same_v<T, int128_t>) {
      auto decimalType = DECIMAL(25, 5);
      return {makeRowVector({"c0"}, {makeFlatVector<int128_t>(values, decimalType)})};
    } else {
      return {makeRowVector({"c0"}, {makeFlatVector<std::string>(values)})};
    }
  }

  template<typename T1, typename T2>
  std::vector<RowVectorPtr> createMultiColumnDataVectors(
      const std::vector<T1>& values1, const std::vector<T2>& values2) {
    return {makeRowVector({"c0", "c1"}, {createVector<T1>(values1), createVector<T2>(values2)})};
  }

  template<typename T>
  VectorPtr createVector(const std::vector<T>& values) {
    if constexpr (std::is_same_v<T, int8_t>) {
      return makeFlatVector<int8_t>(values);
    } else if constexpr (std::is_same_v<T, int16_t>) {
      return makeFlatVector<int16_t>(values);
    } else if constexpr (std::is_same_v<T, int32_t>) {
      return makeFlatVector<int32_t>(values);
    } else if constexpr (std::is_same_v<T, int64_t>) {
      return makeFlatVector<int64_t>(values);
    } else if constexpr (std::is_same_v<T, std::string>) {
      return makeFlatVector<std::string>(values);
    } else {
      return makeFlatVector<std::string>(values);
    }
  }

  template<typename T>
  std::string buildSingleColumnSql(const std::vector<T>& deleteValues, int32_t fieldId, bool isVarbinary = false) {
    if (deleteValues.empty()) {
      return "SELECT * FROM tmp";
    }

    std::string columnName = fmt::format("c{}", fieldId - 1);
    std::string notInClause;

    if (isVarbinary) {
      notInClause = "hex(" + columnName + ") NOT IN (";
      for (size_t i = 0; i < deleteValues.size(); ++i) {
        if (i > 0) notInClause += ", ";
        notInClause += "'";
        if constexpr (std::is_same_v<T, std::string>) {
          for (unsigned char c : deleteValues[i]) {
            notInClause += fmt::format("{:02X}", c);
          }
        }
        notInClause += "'";
      }
    } else {
      notInClause = columnName + " NOT IN (";
      for (size_t i = 0; i < deleteValues.size(); ++i) {
        if (i > 0) notInClause += ", ";
        notInClause += TypeTestHelper::formatValueForSql(deleteValues[i]);
      }
    }
    notInClause += ")";

    return "SELECT * FROM tmp WHERE " + notInClause;
  }

  template<typename T1, typename T2>
  std::string buildMultiColumnSql(
      const std::vector<T1>& deleteValues1, const std::vector<T2>& deleteValues2,
      int32_t fieldId1, int32_t fieldId2) {
    if (deleteValues1.empty() || deleteValues2.empty()) {
      return "SELECT * FROM tmp";
    }

    std::string sql = "SELECT * FROM tmp WHERE ";
    for (size_t i = 0; i < deleteValues1.size() && i < deleteValues2.size(); ++i) {
      if (i > 0) sql += " AND ";
      sql += fmt::format("(c{} <> {} OR c{} <> {})",
                        fieldId1 - 1, TypeTestHelper::formatValueForSql(deleteValues1[i]),
                        fieldId2 - 1, TypeTestHelper::formatValueForSql(deleteValues2[i]));
    }
    return sql;
  }
};

// Update the parameterized tests with simpler type names
class SingleColumnEqualityDeleteTest
    : public HiveIcebergEqualityDeletesTest,
      public ::testing::WithParamInterface<std::pair<std::string, HiveIcebergEqualityDeletesTest::TestScenario>> {};

INSTANTIATE_TEST_SUITE_P(
    AllSingleColumnTypes,
    SingleColumnEqualityDeleteTest,
    ::testing::Values(
        std::make_pair("Tinyint_DeleteSubset", HiveIcebergEqualityDeletesTest::TestScenario::DELETE_SUBSET),
        std::make_pair("Smallint_DeleteSubset", HiveIcebergEqualityDeletesTest::TestScenario::DELETE_SUBSET),
        std::make_pair("Integer_DeleteSubset", HiveIcebergEqualityDeletesTest::TestScenario::DELETE_SUBSET),
        std::make_pair("Bigint_DeleteSubset", HiveIcebergEqualityDeletesTest::TestScenario::DELETE_SUBSET),
        std::make_pair("Varchar_DeleteSubset", HiveIcebergEqualityDeletesTest::TestScenario::DELETE_SUBSET),
        std::make_pair("Tinyint_DeleteAll", HiveIcebergEqualityDeletesTest::TestScenario::DELETE_ALL),
        std::make_pair("Varchar_DeleteNone", HiveIcebergEqualityDeletesTest::TestScenario::DELETE_NONE)
    ),
    [](const ::testing::TestParamInfo<SingleColumnEqualityDeleteTest::ParamType>& info) {
        return info.param.first;
    });

TEST_P(SingleColumnEqualityDeleteTest, SingleColumnTests) {
  folly::SingletonVault::singleton()->registrationComplete();

  auto [testName, scenario] = GetParam();

  if (testName.find("Tinyint_") != std::string::npos) {
    testSingleColumnEqualityDeletes<int8_t>(scenario);
  } else if (testName.find("Smallint_") != std::string::npos) {
    testSingleColumnEqualityDeletes<int16_t>(scenario);
  } else if (testName.find("Integer_") != std::string::npos) {
    testSingleColumnEqualityDeletes<int32_t>(scenario);
  } else if (testName.find("Bigint_") != std::string::npos) {
    testSingleColumnEqualityDeletes<int64_t>(scenario);
  } else if (testName.find("Varchar_") != std::string::npos) {
    testSingleColumnEqualityDeletes<std::string>(scenario);
  }
}

class MultiColumnEqualityDeleteTest
    : public HiveIcebergEqualityDeletesTest,
      public ::testing::WithParamInterface<std::pair<std::string, HiveIcebergEqualityDeletesTest::TestScenario>> {};

INSTANTIATE_TEST_SUITE_P(
    AllMultiColumnTypes,
    MultiColumnEqualityDeleteTest,
    ::testing::Values(
        std::make_pair("Integer_Varchar_DeleteSubset", HiveIcebergEqualityDeletesTest::TestScenario::DELETE_SUBSET),
        std::make_pair("Bigint_Varchar_DeleteSubset", HiveIcebergEqualityDeletesTest::TestScenario::DELETE_SUBSET),
        std::make_pair("Integer_Varchar_DeleteAll", HiveIcebergEqualityDeletesTest::TestScenario::DELETE_ALL),
        std::make_pair("Integer_Varchar_DeleteNone", HiveIcebergEqualityDeletesTest::TestScenario::DELETE_NONE)
    ),
    [](const ::testing::TestParamInfo<MultiColumnEqualityDeleteTest::ParamType>& info) {
        return info.param.first;
    });

TEST_P(MultiColumnEqualityDeleteTest, MultiColumnTests) {
  folly::SingletonVault::singleton()->registrationComplete();

  auto [testName, scenario] = GetParam();

  if (testName.find("Integer_Varchar_") != std::string::npos) {
    testMultiColumnEqualityDeletes<int32_t, std::string>(scenario);
  } else if (testName.find("Bigint_Varchar_") != std::string::npos) {
    testMultiColumnEqualityDeletes<int64_t, std::string>(scenario);
  }
}

class ErrorThrowingTypeTest
    : public HiveIcebergEqualityDeletesTest,
      public ::testing::WithParamInterface<std::string> {};

INSTANTIATE_TEST_SUITE_P(
    AllErrorThrowingTypes,
    ErrorThrowingTypeTest,
    ::testing::Values("Float", "Double", "LongDecimal"),
    [](const ::testing::TestParamInfo<ErrorThrowingTypeTest::ParamType>& info) {
        return info.param;
    });

TEST_P(ErrorThrowingTypeTest, ErrorThrowingTests) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::string typeName = GetParam();

  if (typeName == "Float") {
    testErrorThrowingType<float>();
  } else if (typeName == "Double") {
    testErrorThrowingType<double>();
  } else if (typeName == "LongDecimal") {
    testErrorThrowingType<int128_t>();
  }
}

} // namespace facebook::velox::connector::hive::iceberg
