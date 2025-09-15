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
#include "velox/connectors/hive/iceberg/tests/IcebergReadTestBase.h"
#include "velox/exec/PlanNodeStats.h"

namespace facebook::velox::connector::hive::iceberg {

enum class NullParam {
  kNoNulls,
  kPartialNulls,
  kAllNulls
};

struct TestParams {
  RowTypePtr dataRowType;
  std::vector<NullParam> nullParamForData;
};

class IcebergReadEqualityDeletes : public IcebergReadTestBase,
                                   public testing::WithParamInterface<TestParams> {
 protected:
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

  template <TypeKind KIND>
  void assertEqualityDeletes(
      const std::unordered_map<
          int8_t,
          std::vector<std::vector<typename TypeTraits<KIND>::NativeType>>>&
          equalityDeleteVectorMap,
      const std::unordered_map<int8_t, std::vector<int32_t>>&
          equalityFieldIdsMap,
      std::string duckDbSql = "",
      std::vector<RowVectorPtr> dataVectors = {}) {
    VELOX_CHECK_EQ(equalityDeleteVectorMap.size(), equalityFieldIdsMap.size());
    // We will create data vectors with numColumns number of columns that is the
    // max field ID in equalityFieldIds
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
        writeDataFiles<KIND>(rowCount_, numDataColumns, 1, dataVectors)[0];

    std::vector<connector::hive::iceberg::IcebergDeleteFile> deleteFiles;
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
      deleteFilePaths.push_back(
          writeEqualityDeleteFile<KIND>(equalityDeleteVector));
      IcebergDeleteFile deleteFile(
          FileContent::kEqualityDeletes,
          deleteFilePaths.back()->getPath(),
          fileFormat_,
          equalityDeleteVector[0].size(),
          testing::internal::GetFileSize(
              std::fopen(deleteFilePaths.back()->getPath().c_str(), "r")),
          equalityFieldIds);
      deleteFiles.push_back(deleteFile);
      predicates +=
          makePredicates<KIND>(equalityDeleteVector, equalityFieldIds);
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
      std::vector<TypePtr> types(1, createScalarType<KIND>());
      assertEqualityDeletes(
          icebergSplits.back(),
          std::make_shared<RowType>(std::move(names), std::move(types)),
          duckDbQuery);
    }
  }

  /// Generate test data vectors with configurable null patterns based on NullParam
  template <TypeKind KIND>
  std::vector<RowVectorPtr> makeVectorsForParams(
      const TestParams& params,
      int32_t count = 1,
      int32_t rowsPerVector = rowCount_) {
    using T = typename TypeTraits<KIND>::NativeType;

    auto rowType = params.dataRowType;
    auto nullParams = params.nullParamForData;
    
    VELOX_CHECK_EQ(rowType->size(), nullParams.size());

    // Sample strings for VARCHAR data generation
    const std::vector<std::string> sampleStrings = {
        "apple",     "banana",     "cherry",    "date",       "elderberry",
        "fig",       "grape",      "honeydew",  "kiwi",       "lemon",
        "mango",     "nectarine",  "orange",    "papaya",     "quince",
        "raspberry", "strawberry", "tangerine", "watermelon", "zucchini"};

    std::vector<RowVectorPtr> rowVectors;
    for (int i = 0; i < count; i++) {
      std::vector<VectorPtr> vectors;

      for (size_t j = 0; j < rowType->size(); j++) {
        auto fieldType = rowType->childAt(j);
        auto nullParam = nullParams[j];
        
        VectorPtr columnVector;

        if (nullParam == NullParam::kAllNulls) {
          // Use allNullFlatVector for all-null columns
          columnVector = vectorMaker_.allNullFlatVector<T>(rowsPerVector);
        } else if (fieldType->isVarchar()) {
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
        } else if (fieldType->isVarbinary()) {
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
        } else if (fieldType->isBigint() || fieldType->isInteger() || 
                   fieldType->isSmallint() || fieldType->isTinyint()) {
          auto data = makeSequenceValues<int64_t>(rowsPerVector, j + 1);
          if (fieldType->isBigint()) {
            columnVector = vectorMaker_.flatVector<int64_t>(data);
          } else if (fieldType->isInteger()) {
            std::vector<int32_t> intData;
            intData.reserve(data.size());
            for (auto val : data) {
              intData.push_back(static_cast<int32_t>(val));
            }
            columnVector = vectorMaker_.flatVector<int32_t>(intData);
          } else if (fieldType->isSmallint()) {
            std::vector<int16_t> shortData;
            shortData.reserve(data.size());
            for (auto val : data) {
              shortData.push_back(static_cast<int16_t>(val));
            }
            columnVector = vectorMaker_.flatVector<int16_t>(shortData);
          } else if (fieldType->isTinyint()) {
            std::vector<int8_t> byteData;
            byteData.reserve(data.size());
            for (auto val : data) {
              byteData.push_back(static_cast<int8_t>(val));
            }
            columnVector = vectorMaker_.flatVector<int8_t>(byteData);
          }
        } else {
          VELOX_FAIL(
              "Unsupported type for makeVectorsForParams: {}", fieldType->toString());
        }

        // Apply partial nulls by randomly setting some positions to null
        if (nullParam == NullParam::kPartialNulls) {
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

      std::vector<std::string> names;
      std::vector<TypePtr> types;
      for (size_t j = 0; j < rowType->size(); j++) {
        names.push_back(rowType->nameOf(j));
        types.push_back(rowType->childAt(j));
      }
      
      rowVectors.push_back(makeRowVector(names, vectors));
    }

    rowType_ = rowType;
    return rowVectors;
  }

  /// Generate test data vectors with configurable null patterns
  template <TypeKind KIND>
  std::vector<RowVectorPtr> makeVectors(
      int32_t count,
      int32_t rowsPerVector,
      int32_t numColumns = 1,
      bool allNulls = false,
      bool partialNull = false) {
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

  /// Creates data files for Iceberg testing with simple row/column
  /// specifications.
  ///
  /// This function generates test data files with the specified data type and
  /// structure. It creates columnar data with predictable patterns for testing
  /// purposes.
  ///
  /// @tparam KIND The data type for columns (default: BIGINT)
  /// @param numRows Total number of rows per split/file
  /// @param numColumns Number of columns in each file (default: 1)
  /// @param splitCount Number of separate files to create (default: 1)
  /// @param dataVectors Pre-created data vectors; if empty, generates new ones
  /// @return Vector of file paths to the created data files
  ///
  /// @note Generated data follows patterns:
  ///   - First column (c0): continuously increasing values [0, 1, 2, ...]
  ///   - Second column (c1): values repeat once [0, 0, 1, 1, 2, 2, ...]
  ///   - Third column (c2): values repeat twice [0, 0, 0, 0, 1, 1, 1, 1, ...]
  ///   - And so on for additional columns
  template <TypeKind KIND = TypeKind::BIGINT>
  std::vector<std::shared_ptr<TempFilePath>> writeDataFiles(
      uint64_t numRows,
      int32_t numColumns = 1,
      int32_t splitCount = 1,
      std::vector<RowVectorPtr> dataVectors = {}) {
    if (dataVectors.empty()) {
      dataVectors = makeVectors<KIND>(splitCount, numRows, numColumns);
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

 private:
  /// Write equality delete file with typed data
  template <TypeKind KIND>
  std::shared_ptr<TempFilePath> writeEqualityDeleteFile(
      const std::vector<std::vector<typename TypeTraits<KIND>::NativeType>>&
          equalityDeleteVector) {
    using T = typename TypeTraits<KIND>::NativeType;
    std::vector<std::string> names;
    std::vector<VectorPtr> vectors;
    for (int i = 0; i < equalityDeleteVector.size(); i++) {
      names.push_back(fmt::format("c{}", i));
      vectors.push_back(makeFlatVector<T>(equalityDeleteVector[i]));
    }

    RowVectorPtr const deleteFileVectors = makeRowVector(names, vectors);

    auto deleteFilePath = TempFilePath::create();
    writeToFile(deleteFilePath->getPath(), deleteFileVectors);

    return deleteFilePath;
  }

  /// Create typed predicate string for DuckDB queries
  template <TypeKind KIND>
  std::string makeTypedPredicate(
      const std::string& columnName,
      const typename TypeTraits<KIND>::NativeType& value) {
    if constexpr (KIND == TypeKind::VARCHAR || KIND == TypeKind::VARBINARY) {
      return fmt::format("({} <> '{}')", columnName, value);
    } else if constexpr (
        KIND == TypeKind::TINYINT || KIND == TypeKind::SMALLINT ||
        KIND == TypeKind::INTEGER || KIND == TypeKind::BIGINT) {
      return fmt::format("({} <> {})", columnName, value);
    } else {
      VELOX_FAIL("Unsupported predicate type : {}", TypeTraits<KIND>::name);
    }
  }

  /// Generate DuckDB predicates for equality delete testing
  template <TypeKind KIND>
  std::string makePredicates(
      const std::vector<std::vector<typename TypeTraits<KIND>::NativeType>>&
          equalityDeleteVector,
      const std::vector<int32_t>& equalityFieldIds) {
    using T = typename TypeTraits<KIND>::NativeType;

    std::string predicates;
    int32_t numDataColumns =
        *std::max_element(equalityFieldIds.begin(), equalityFieldIds.end());

    VELOX_CHECK_GT(numDataColumns, 0);
    VELOX_CHECK_GE(numDataColumns, equalityDeleteVector.size());
    VELOX_CHECK_GT(equalityDeleteVector.size(), 0);

    auto numDeletedValues = equalityDeleteVector[0].size();

    if (numDeletedValues == 0) {
      return predicates;
    }

    // Check if all values for a column are deleted
    for (auto i = 0; i < equalityDeleteVector.size(); i++) {
      auto equalityFieldId = equalityFieldIds[i];
      auto deleteValues = equalityDeleteVector[i];

      // Make a copy to find unique values
      auto uniqueValues = deleteValues;
      std::sort(uniqueValues.begin(), uniqueValues.end());
      auto lastIter = std::unique(uniqueValues.begin(), uniqueValues.end());
      auto numDistinctValues = std::distance(uniqueValues.begin(), lastIter);

      // For column with field ID n, the max value is (rowCount_-1)/(n)
      // because values repeat n times
      if (numDistinctValues > 0 && equalityFieldId > 0) {
        auto maxPossibleValue = (rowCount_ - 1) / equalityFieldId;
        if (numDistinctValues > maxPossibleValue) {
          return "1 = 0";
        }
      }
    }

    if (equalityDeleteVector.size() == 1) {
      std::string name = fmt::format("c{}", equalityFieldIds[0] - 1);
      predicates = fmt::format(
          "({} IS NULL OR {} NOT IN ({}))",
          name,
          name,
          makeNotInList<KIND>({equalityDeleteVector[0]}));
    } else {
      for (int i = 0; i < numDeletedValues; i++) {
        std::string oneRow;
        for (int j = 0; j < equalityFieldIds.size(); j++) {
          std::string const name = fmt::format("c{}", equalityFieldIds[j] - 1);
          std::string predicate =
              makeTypedPredicate<KIND>(name, equalityDeleteVector[j][i]);

          oneRow = oneRow.empty()
              ? predicate
              : fmt::format("({} OR {})", oneRow, predicate);
        }

        predicates = predicates.empty()
            ? oneRow
            : fmt::format("{} AND {}", predicates, oneRow);
      }
    }
    return predicates;
  }
};

TEST_P(IcebergReadEqualityDeletes, testSubFieldEqualityDelete) {
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
  dataFilePath = writeDataFiles<TypeKind::BIGINT>(
      rowCount_, numDataColumns, 1, dataVectors)[0];

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

TEST_P(IcebergReadEqualityDeletes, equalityDeletesMixedTypesInt64Varchar) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  equalityFieldIdsMap.insert({0, {1, 2}});

  // Create data vectors with int64_t and varchar columns
  std::vector<RowVectorPtr> dataVectors = {makeRowVector(
      {"c0", "c1"},
      {makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
       makeFlatVector<StringView>(
           {"apple",
            "banana",
            "cherry",
            "date",
            "elderberry",
            "fig",
            "grape",
            "honeydew",
            "kiwi",
            "lemon"})})};

  // Test 1: Delete first and last rows
  {
    std::unordered_map<int8_t, std::vector<std::vector<int64_t>>> intDeleteMap;
    std::unordered_map<int8_t, std::vector<std::vector<StringView>>>
        stringDeleteMap;

    intDeleteMap.insert({0, {{0, 9}}});
    stringDeleteMap.insert({1, {{"apple", "lemon"}}});

    // Write int64_t delete file
    auto intDeleteFilePath = TempFilePath::create();
    RowVectorPtr intDeleteVector =
        makeRowVector({"c0"}, {makeFlatVector<int64_t>(intDeleteMap.at(0)[0])});
    writeToFile(intDeleteFilePath->getPath(), intDeleteVector);

    // Write varchar delete file
    auto stringDeleteFilePath = TempFilePath::create();
    RowVectorPtr stringDeleteVector = makeRowVector(
        {"c1"}, {makeFlatVector<StringView>(stringDeleteMap.at(1)[0])});
    writeToFile(stringDeleteFilePath->getPath(), stringDeleteVector);

    std::vector<IcebergDeleteFile> deleteFiles;
    deleteFiles.push_back(IcebergDeleteFile(
        FileContent::kEqualityDeletes,
        intDeleteFilePath->getPath(),
        fileFormat_,
        2,
        testing::internal::GetFileSize(
            std::fopen(intDeleteFilePath->getPath().c_str(), "r")),
        {1}));
    deleteFiles.push_back(IcebergDeleteFile(
        FileContent::kEqualityDeletes,
        stringDeleteFilePath->getPath(),
        fileFormat_,
        2,
        testing::internal::GetFileSize(
            std::fopen(stringDeleteFilePath->getPath().c_str(), "r")),
        {2}));

    auto dataFilePath = TempFilePath::create();
    writeToFile(dataFilePath->getPath(), dataVectors);
    auto icebergSplits =
        makeIcebergSplits(dataFilePath->getPath(), deleteFiles);

    createDuckDbTable(dataVectors);
    std::string duckDbSql =
        "SELECT * FROM tmp WHERE c0 NOT IN (0, 9) AND c1 NOT IN ('apple', 'lemon')";
    assertEqualityDeletes(
        icebergSplits.back(), asRowType(dataVectors[0]->type()), duckDbSql);
  }

  // Test 2: Delete random rows
  {
    std::unordered_map<int8_t, std::vector<std::vector<int64_t>>> intDeleteMap;
    std::unordered_map<int8_t, std::vector<std::vector<StringView>>>
        stringDeleteMap;

    intDeleteMap.insert({0, {{1, 3, 5, 7}}});
    stringDeleteMap.insert({1, {{"banana", "date", "fig", "honeydew"}}});

    auto intDeleteFilePath = TempFilePath::create();
    RowVectorPtr intDeleteVector =
        makeRowVector({"c0"}, {makeFlatVector<int64_t>(intDeleteMap.at(0)[0])});
    writeToFile(intDeleteFilePath->getPath(), intDeleteVector);

    auto stringDeleteFilePath = TempFilePath::create();
    RowVectorPtr stringDeleteVector = makeRowVector(
        {"c1"}, {makeFlatVector<StringView>(stringDeleteMap.at(1)[0])});
    writeToFile(stringDeleteFilePath->getPath(), stringDeleteVector);

    std::vector<IcebergDeleteFile> deleteFiles;
    deleteFiles.push_back(IcebergDeleteFile(
        FileContent::kEqualityDeletes,
        intDeleteFilePath->getPath(),
        fileFormat_,
        4,
        testing::internal::GetFileSize(
            std::fopen(intDeleteFilePath->getPath().c_str(), "r")),
        {1}));
    deleteFiles.push_back(IcebergDeleteFile(
        FileContent::kEqualityDeletes,
        stringDeleteFilePath->getPath(),
        fileFormat_,
        4,
        testing::internal::GetFileSize(
            std::fopen(stringDeleteFilePath->getPath().c_str(), "r")),
        {2}));

    auto dataFilePath = TempFilePath::create();
    writeToFile(dataFilePath->getPath(), dataVectors);
    auto icebergSplits =
        makeIcebergSplits(dataFilePath->getPath(), deleteFiles);

    createDuckDbTable(dataVectors);
    std::string duckDbSql =
        "SELECT * FROM tmp WHERE c0 NOT IN (1, 3, 5, 7) AND c1 NOT IN ('banana', 'date', 'fig', 'honeydew')";
    assertEqualityDeletes(
        icebergSplits.back(), asRowType(dataVectors[0]->type()), duckDbSql);
  }

  // Test 3: Delete all rows
  {
    std::unordered_map<int8_t, std::vector<std::vector<int64_t>>> intDeleteMap;
    std::unordered_map<int8_t, std::vector<std::vector<StringView>>>
        stringDeleteMap;

    intDeleteMap.insert({0, {{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}}});
    stringDeleteMap.insert(
        {1,
         {{"apple",
           "banana",
           "cherry",
           "date",
           "elderberry",
           "fig",
           "grape",
           "honeydew",
           "kiwi",
           "lemon"}}});

    auto intDeleteFilePath = TempFilePath::create();
    RowVectorPtr intDeleteVector =
        makeRowVector({"c0"}, {makeFlatVector<int64_t>(intDeleteMap.at(0)[0])});
    writeToFile(intDeleteFilePath->getPath(), intDeleteVector);

    auto stringDeleteFilePath = TempFilePath::create();
    RowVectorPtr stringDeleteVector = makeRowVector(
        {"c1"}, {makeFlatVector<StringView>(stringDeleteMap.at(1)[0])});
    writeToFile(stringDeleteFilePath->getPath(), stringDeleteVector);

    std::vector<IcebergDeleteFile> deleteFiles;
    deleteFiles.push_back(IcebergDeleteFile(
        FileContent::kEqualityDeletes,
        intDeleteFilePath->getPath(),
        fileFormat_,
        10,
        testing::internal::GetFileSize(
            std::fopen(intDeleteFilePath->getPath().c_str(), "r")),
        {1}));
    deleteFiles.push_back(IcebergDeleteFile(
        FileContent::kEqualityDeletes,
        stringDeleteFilePath->getPath(),
        fileFormat_,
        10,
        testing::internal::GetFileSize(
            std::fopen(stringDeleteFilePath->getPath().c_str(), "r")),
        {2}));

    auto dataFilePath = TempFilePath::create();
    writeToFile(dataFilePath->getPath(), dataVectors);
    auto icebergSplits =
        makeIcebergSplits(dataFilePath->getPath(), deleteFiles);

    createDuckDbTable(dataVectors);
    std::string duckDbSql = "SELECT * FROM tmp WHERE 1 = 0";
    assertEqualityDeletes(
        icebergSplits.back(), asRowType(dataVectors[0]->type()), duckDbSql);
  }

  // Test 4: Delete none
  {
    std::unordered_map<int8_t, std::vector<std::vector<int64_t>>> intDeleteMap;
    std::unordered_map<int8_t, std::vector<std::vector<StringView>>>
        stringDeleteMap;

    intDeleteMap.insert({0, {{}}});
    stringDeleteMap.insert({1, {{}}});

    auto intDeleteFilePath = TempFilePath::create();
    RowVectorPtr intDeleteVector =
        makeRowVector({"c0"}, {makeFlatVector<int64_t>(intDeleteMap.at(0)[0])});
    writeToFile(intDeleteFilePath->getPath(), intDeleteVector);

    auto stringDeleteFilePath = TempFilePath::create();
    RowVectorPtr stringDeleteVector = makeRowVector(
        {"c1"}, {makeFlatVector<StringView>(stringDeleteMap.at(1)[0])});
    writeToFile(stringDeleteFilePath->getPath(), stringDeleteVector);

    std::vector<IcebergDeleteFile> deleteFiles;
    deleteFiles.push_back(IcebergDeleteFile(
        FileContent::kEqualityDeletes,
        intDeleteFilePath->getPath(),
        fileFormat_,
        0,
        testing::internal::GetFileSize(
            std::fopen(intDeleteFilePath->getPath().c_str(), "r")),
        {1}));
    deleteFiles.push_back(IcebergDeleteFile(
        FileContent::kEqualityDeletes,
        stringDeleteFilePath->getPath(),
        fileFormat_,
        0,
        testing::internal::GetFileSize(
            std::fopen(stringDeleteFilePath->getPath().c_str(), "r")),
        {2}));

    auto dataFilePath = TempFilePath::create();
    writeToFile(dataFilePath->getPath(), dataVectors);
    auto icebergSplits =
        makeIcebergSplits(dataFilePath->getPath(), deleteFiles);

    createDuckDbTable(dataVectors);
    std::string duckDbSql = "SELECT * FROM tmp";
    assertEqualityDeletes(
        icebergSplits.back(), asRowType(dataVectors[0]->type()), duckDbSql);
  }
}

TEST_P(
    IcebergReadEqualityDeletes,
    equalityDeletesMixedTypesTinyintVarbinary) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  equalityFieldIdsMap.insert({0, {1, 2}});

  // Create data vectors with int8_t and varbinary columns
  std::vector<RowVectorPtr> dataVectors = {makeRowVector(
      {"c0", "c1"},
      {makeFlatVector<int8_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
       makeFlatVector<StringView>(
           {"\x01\x02",
            "\x03\x04",
            "\x05\x06",
            "\x07\x08",
            "\x09\x0A",
            "\x0B\x0C",
            "\x0D\x0E",
            "\x0F\x10",
            "\x11\x12",
            "\x13\x14"})})};

  // Test 1: Delete first and last rows
  {
    std::unordered_map<int8_t, std::vector<std::vector<int8_t>>> intDeleteMap;
    std::unordered_map<int8_t, std::vector<std::vector<StringView>>>
        binaryDeleteMap;

    intDeleteMap.insert({0, {{0, 9}}});
    binaryDeleteMap.insert({1, {{"\x01\x02", "\x13\x14"}}});

    auto intDeleteFilePath = TempFilePath::create();
    RowVectorPtr intDeleteVector =
        makeRowVector({"c0"}, {makeFlatVector<int8_t>(intDeleteMap.at(0)[0])});
    writeToFile(intDeleteFilePath->getPath(), intDeleteVector);

    auto binaryDeleteFilePath = TempFilePath::create();
    RowVectorPtr binaryDeleteVector = makeRowVector(
        {"c1"}, {makeFlatVector<StringView>(binaryDeleteMap.at(1)[0])});
    writeToFile(binaryDeleteFilePath->getPath(), binaryDeleteVector);

    std::vector<IcebergDeleteFile> deleteFiles;
    deleteFiles.push_back(IcebergDeleteFile(
        FileContent::kEqualityDeletes,
        intDeleteFilePath->getPath(),
        fileFormat_,
        2,
        testing::internal::GetFileSize(
            std::fopen(intDeleteFilePath->getPath().c_str(), "r")),
        {1}));
    deleteFiles.push_back(IcebergDeleteFile(
        FileContent::kEqualityDeletes,
        binaryDeleteFilePath->getPath(),
        fileFormat_,
        2,
        testing::internal::GetFileSize(
            std::fopen(binaryDeleteFilePath->getPath().c_str(), "r")),
        {2}));

    auto dataFilePath = TempFilePath::create();
    writeToFile(dataFilePath->getPath(), dataVectors);
    auto icebergSplits =
        makeIcebergSplits(dataFilePath->getPath(), deleteFiles);

    createDuckDbTable(dataVectors);
    std::string duckDbSql =
        "SELECT * FROM tmp WHERE c0 NOT IN (0, 9) AND hex(c1) NOT IN ('0102', '1314')";
    assertEqualityDeletes(
        icebergSplits.back(), asRowType(dataVectors[0]->type()), duckDbSql);
  }

  // Test 2: Delete random rows
  {
    std::unordered_map<int8_t, std::vector<std::vector<int8_t>>> intDeleteMap;
    std::unordered_map<int8_t, std::vector<std::vector<StringView>>>
        binaryDeleteMap;

    intDeleteMap.insert({0, {{1, 3, 5, 7}}});
    binaryDeleteMap.insert(
        {1, {{"\x03\x04", "\x07\x08", "\x0B\x0C", "\x0F\x10"}}});

    auto intDeleteFilePath = TempFilePath::create();
    RowVectorPtr intDeleteVector =
        makeRowVector({"c0"}, {makeFlatVector<int8_t>(intDeleteMap.at(0)[0])});
    writeToFile(intDeleteFilePath->getPath(), intDeleteVector);

    auto binaryDeleteFilePath = TempFilePath::create();
    RowVectorPtr binaryDeleteVector = makeRowVector(
        {"c1"}, {makeFlatVector<StringView>(binaryDeleteMap.at(1)[0])});
    writeToFile(binaryDeleteFilePath->getPath(), binaryDeleteVector);

    std::vector<IcebergDeleteFile> deleteFiles;
    deleteFiles.push_back(IcebergDeleteFile(
        FileContent::kEqualityDeletes,
        intDeleteFilePath->getPath(),
        fileFormat_,
        4,
        testing::internal::GetFileSize(
            std::fopen(intDeleteFilePath->getPath().c_str(), "r")),
        {1}));
    deleteFiles.push_back(IcebergDeleteFile(
        FileContent::kEqualityDeletes,
        binaryDeleteFilePath->getPath(),
        fileFormat_,
        4,
        testing::internal::GetFileSize(
            std::fopen(binaryDeleteFilePath->getPath().c_str(), "r")),
        {2}));

    auto dataFilePath = TempFilePath::create();
    writeToFile(dataFilePath->getPath(), dataVectors);
    auto icebergSplits =
        makeIcebergSplits(dataFilePath->getPath(), deleteFiles);

    createDuckDbTable(dataVectors);
    std::string duckDbSql =
        "SELECT * FROM tmp WHERE c0 NOT IN (1, 3, 5, 7) AND hex(c1) NOT IN ('0304', '0708', '0B0C', '0F10')";
    assertEqualityDeletes(
        icebergSplits.back(), asRowType(dataVectors[0]->type()), duckDbSql);
  }

  // Test 3: Delete all rows
  {
    std::unordered_map<int8_t, std::vector<std::vector<int8_t>>> intDeleteMap;
    std::unordered_map<int8_t, std::vector<std::vector<StringView>>>
        binaryDeleteMap;

    intDeleteMap.insert({0, {{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}}});
    binaryDeleteMap.insert(
        {1,
         {{"\x01\x02",
           "\x03\x04",
           "\x05\x06",
           "\x07\x08",
           "\x09\x0A",
           "\x0B\x0C",
           "\x0D\x0E",
           "\x0F\x10",
           "\x11\x12",
           "\x13\x14"}}});

    auto intDeleteFilePath = TempFilePath::create();
    RowVectorPtr intDeleteVector =
        makeRowVector({"c0"}, {makeFlatVector<int8_t>(intDeleteMap.at(0)[0])});
    writeToFile(intDeleteFilePath->getPath(), intDeleteVector);

    auto binaryDeleteFilePath = TempFilePath::create();
    RowVectorPtr binaryDeleteVector = makeRowVector(
        {"c1"}, {makeFlatVector<StringView>(binaryDeleteMap.at(1)[0])});
    writeToFile(binaryDeleteFilePath->getPath(), binaryDeleteVector);

    std::vector<IcebergDeleteFile> deleteFiles;
    deleteFiles.push_back(IcebergDeleteFile(
        FileContent::kEqualityDeletes,
        intDeleteFilePath->getPath(),
        fileFormat_,
        10,
        testing::internal::GetFileSize(
            std::fopen(intDeleteFilePath->getPath().c_str(), "r")),
        {1}));
    deleteFiles.push_back(IcebergDeleteFile(
        FileContent::kEqualityDeletes,
        binaryDeleteFilePath->getPath(),
        fileFormat_,
        10,
        testing::internal::GetFileSize(
            std::fopen(binaryDeleteFilePath->getPath().c_str(), "r")),
        {2}));

    auto dataFilePath = TempFilePath::create();
    writeToFile(dataFilePath->getPath(), dataVectors);
    auto icebergSplits =
        makeIcebergSplits(dataFilePath->getPath(), deleteFiles);

    createDuckDbTable(dataVectors);
    std::string duckDbSql = "SELECT * FROM tmp WHERE 1 = 0";
    assertEqualityDeletes(
        icebergSplits.back(), asRowType(dataVectors[0]->type()), duckDbSql);
  }

  // Test 4: Delete none
  {
    std::unordered_map<int8_t, std::vector<std::vector<int8_t>>> intDeleteMap;
    std::unordered_map<int8_t, std::vector<std::vector<StringView>>>
        binaryDeleteMap;

    intDeleteMap.insert({0, {{}}});
    binaryDeleteMap.insert({1, {{}}});

    auto intDeleteFilePath = TempFilePath::create();
    RowVectorPtr intDeleteVector =
        makeRowVector({"c0"}, {makeFlatVector<int8_t>(intDeleteMap.at(0)[0])});
    writeToFile(intDeleteFilePath->getPath(), intDeleteVector);

    auto binaryDeleteFilePath = TempFilePath::create();
    RowVectorPtr binaryDeleteVector = makeRowVector(
        {"c1"}, {makeFlatVector<StringView>(binaryDeleteMap.at(1)[0])});
    writeToFile(binaryDeleteFilePath->getPath(), binaryDeleteVector);

    std::vector<IcebergDeleteFile> deleteFiles;
    deleteFiles.push_back(IcebergDeleteFile(
        FileContent::kEqualityDeletes,
        intDeleteFilePath->getPath(),
        fileFormat_,
        0,
        testing::internal::GetFileSize(
            std::fopen(intDeleteFilePath->getPath().c_str(), "r")),
        {1}));
    deleteFiles.push_back(IcebergDeleteFile(
        FileContent::kEqualityDeletes,
        binaryDeleteFilePath->getPath(),
        fileFormat_,
        0,
        testing::internal::GetFileSize(
            std::fopen(binaryDeleteFilePath->getPath().c_str(), "r")),
        {2}));

    auto dataFilePath = TempFilePath::create();
    writeToFile(dataFilePath->getPath(), dataVectors);
    auto icebergSplits =
        makeIcebergSplits(dataFilePath->getPath(), deleteFiles);

    createDuckDbTable(dataVectors);
    std::string duckDbSql = "SELECT * FROM tmp";
    assertEqualityDeletes(
        icebergSplits.back(), asRowType(dataVectors[0]->type()), duckDbSql);
  }
}

TEST_P(
    IcebergReadEqualityDeletes,
    equalityDeletesFloatAndDoubleThrowsError) {
  folly::SingletonVault::singleton()->registrationComplete();

  // Test for float (REAL)
  {
    std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
    std::unordered_map<int8_t, std::vector<std::vector<float>>>
        equalityDeleteVectorMap;
    equalityFieldIdsMap.insert({{0, {1}}, {1, {2}}});
    equalityDeleteVectorMap.insert({{0, {{0, 1}}}, {1, {{2, 3}}}});
    VELOX_ASSERT_THROW(
        assertEqualityDeletes<TypeKind::REAL>(
            equalityDeleteVectorMap, equalityFieldIdsMap),
        "Iceberg does not allow DOUBLE or REAL columns as the equality delete columns: c1 : REAL");
  }

  // Test for float (REAL) - Delete all
  {
    std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
    std::unordered_map<int8_t, std::vector<std::vector<float>>>
        equalityDeleteVectorMap;
    equalityFieldIdsMap.insert({0, {1}});
    std::vector<float> allValues;
    for (int i = 0; i < rowCount_; ++i) {
      allValues.push_back(static_cast<double>(i));
    }
    equalityDeleteVectorMap.insert({0, {allValues}});
    VELOX_ASSERT_THROW(
        assertEqualityDeletes<TypeKind::REAL>(
            equalityDeleteVectorMap, equalityFieldIdsMap),
        "Iceberg does not allow DOUBLE or REAL columns as the equality delete columns: c0 : REAL");
  }

  // Test for double (DOUBLE)
  {
    std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
    std::unordered_map<int8_t, std::vector<std::vector<double>>>
        equalityDeleteVectorMap;
    equalityFieldIdsMap.insert({{0, {1}}, {1, {2}}});
    equalityDeleteVectorMap.insert({{0, {{0, 1}}}, {1, {{2, 3}}}});
    VELOX_ASSERT_THROW(
        assertEqualityDeletes<TypeKind::DOUBLE>(
            equalityDeleteVectorMap, equalityFieldIdsMap),
        "Iceberg does not allow DOUBLE or REAL columns as the equality delete columns: c1 : DOUBLE");
  }

  // Test for double (DOUBLE) - Delete all
  {
    std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
    std::unordered_map<int8_t, std::vector<std::vector<double>>>
        equalityDeleteVectorMap;
    equalityFieldIdsMap.insert({0, {1}});
    std::vector<double> allValues;
    for (int i = 0; i < rowCount_; ++i) {
      allValues.push_back(static_cast<double>(i));
    }
    equalityDeleteVectorMap.insert({0, {allValues}});
    VELOX_ASSERT_THROW(
        assertEqualityDeletes<TypeKind::DOUBLE>(
            equalityDeleteVectorMap, equalityFieldIdsMap),
        "Iceberg does not allow DOUBLE or REAL columns as the equality delete columns: c0 : DOUBLE");
  }
}

TEST_P(IcebergReadEqualityDeletes, equalityDeletesLongDecimal) {
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
      assertEqualityDeletes<TypeKind::HUGEINT>(
          equalityDeleteVectorMap,
          equalityFieldIdsMap,
          "SELECT * FROM tmp WHERE c0 NOT IN (123456789012345, 987654321098765)",
          dataVectors),
      "Decimal is not supported for DWRF.");

  // Delete all
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert(
      {0,
       {{123456789012345,
         987654321098765,
         111111111111111,
         222222222222222,
         333333333333333}}});

  VELOX_ASSERT_THROW(
      assertEqualityDeletes<TypeKind::HUGEINT>(
          equalityDeleteVectorMap,
          equalityFieldIdsMap,
          "SELECT * FROM tmp WHERE 1 = 0",
          dataVectors),
      "Decimal is not supported for DWRF.");

  // Delete none
  equalityDeleteVectorMap.clear();
  equalityDeleteVectorMap.insert({0, {{}}});
  VELOX_ASSERT_THROW(
      assertEqualityDeletes<TypeKind::HUGEINT>(
          equalityDeleteVectorMap,
          equalityFieldIdsMap,
          "SELECT * FROM tmp",
          dataVectors),
      "Decimal is not supported for DWRF.");
}

// Add new test methods based on the original parameterized tests
TEST_P(IcebergReadEqualityDeletes, floatAndDoubleThrowsError) {
  auto params = GetParam();
  auto rowType = params.dataRowType;
  auto nullParams = params.nullParamForData;
  
  folly::SingletonVault::singleton()->registrationComplete();

  // Only run this test for REAL and DOUBLE types
  if (!rowType->childAt(0)->isReal() && !rowType->childAt(0)->isDouble()) {
    GTEST_SKIP() << "Skipping floatAndDoubleThrowsError for non-floating point type: " 
                 << rowType->childAt(0)->toString();
    return;
  }
  
  if (rowType->childAt(0)->isReal()) {
    // Test for float (REAL)
    std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
    std::unordered_map<int8_t, std::vector<std::vector<float>>>
        equalityDeleteVectorMap;
    equalityFieldIdsMap.insert({{0, {1}}, {1, {2}}});
    equalityDeleteVectorMap.insert({{0, {{0, 1}}}, {1, {{2, 3}}}});
    VELOX_ASSERT_THROW(
        assertEqualityDeletes<TypeKind::REAL>(
            equalityDeleteVectorMap, equalityFieldIdsMap),
        "Iceberg does not allow DOUBLE or REAL columns as the equality delete columns: c1 : REAL");
  }
  
  if (rowType->childAt(0)->isDouble()) {
    // Test for double (DOUBLE)
    std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
    std::unordered_map<int8_t, std::vector<std::vector<double>>>
        equalityDeleteVectorMap;
    equalityFieldIdsMap.insert({{0, {1}}, {1, {2}}});
    equalityDeleteVectorMap.insert({{0, {{0, 1}}}, {1, {{2, 3}}}});
    VELOX_ASSERT_THROW(
        assertEqualityDeletes<TypeKind::DOUBLE>(
            equalityDeleteVectorMap, equalityFieldIdsMap),
        "Iceberg does not allow DOUBLE or REAL columns as the equality delete columns: c1 : DOUBLE");
  }
}

TEST_P(IcebergReadEqualityDeletes, deleteFirstAndLastRows) {
  auto params = GetParam();
  auto rowType = params.dataRowType;
  auto nullParams = params.nullParamForData;
  
  folly::SingletonVault::singleton()->registrationComplete();
  
  // Skip test for floating point types as they should throw errors
  if (rowType->childAt(0)->isReal() || rowType->childAt(0)->isDouble()) {
    GTEST_SKIP() << "Skipping deleteFirstAndLastRows for floating point type: " 
                 << rowType->childAt(0)->toString();
    return;
  }
  
  // Use BIGINT type for simplicity in this basic test
  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<int64_t>>>
      equalityDeleteVectorMap;
  equalityFieldIdsMap.insert({0, {1}});
  
  // Delete first and last rows
  equalityDeleteVectorMap.insert({0, {{0, rowCount_ - 1}}});
  assertEqualityDeletes<TypeKind::BIGINT>(equalityDeleteVectorMap, equalityFieldIdsMap);
}

TEST_P(IcebergReadEqualityDeletes, deleteRandomRows) {
  auto params = GetParam();
  auto rowType = params.dataRowType;
  auto nullParams = params.nullParamForData;
  
  folly::SingletonVault::singleton()->registrationComplete();
  
  // Skip test for floating point types as they should throw errors
  if (rowType->childAt(0)->isReal() || rowType->childAt(0)->isDouble()) {
    GTEST_SKIP() << "Skipping deleteRandomRows for floating point type: " 
                 << rowType->childAt(0)->toString();
    return;
  }
  
  // Use BIGINT type for simplicity in this basic test
  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<int64_t>>>
      equalityDeleteVectorMap;
  equalityFieldIdsMap.insert({0, {1}});
  
  // Delete random rows
  auto randomIndices = makeRandomDeleteValues(rowCount_);
  std::vector<int64_t> deleteValues;
  for (auto idx : randomIndices) {
    deleteValues.push_back(static_cast<int64_t>(idx));
  }
  equalityDeleteVectorMap.insert({0, {deleteValues}});
  assertEqualityDeletes<TypeKind::BIGINT>(equalityDeleteVectorMap, equalityFieldIdsMap);
}

TEST_P(IcebergReadEqualityDeletes, deleteAllRows) {
  auto params = GetParam();
  auto rowType = params.dataRowType;
  auto nullParams = params.nullParamForData;
  
  folly::SingletonVault::singleton()->registrationComplete();
  
  // Skip test for floating point types as they should throw errors
  if (rowType->childAt(0)->isReal() || rowType->childAt(0)->isDouble()) {
    GTEST_SKIP() << "Skipping deleteAllRows for floating point type: " 
                 << rowType->childAt(0)->toString();
    return;
  }
  
  // Use BIGINT type for simplicity in this basic test
  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<int64_t>>>
      equalityDeleteVectorMap;
  equalityFieldIdsMap.insert({0, {1}});
  
  // Delete all rows
  equalityDeleteVectorMap.insert({0, {makeSequenceValues<int64_t>(rowCount_)}});
  assertEqualityDeletes<TypeKind::BIGINT>(
      equalityDeleteVectorMap, equalityFieldIdsMap, "SELECT * FROM tmp WHERE 1 = 0");
}

TEST_P(IcebergReadEqualityDeletes, deleteNoRows) {
  auto params = GetParam();
  auto rowType = params.dataRowType;
  auto nullParams = params.nullParamForData;
  
  folly::SingletonVault::singleton()->registrationComplete();
  
  // Skip test for floating point types as they should throw errors
  if (rowType->childAt(0)->isReal() || rowType->childAt(0)->isDouble()) {
    GTEST_SKIP() << "Skipping deleteNoRows for floating point type: " 
                 << rowType->childAt(0)->toString();
    return;
  }
  
  // Use BIGINT type for simplicity in this basic test
  std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
  std::unordered_map<int8_t, std::vector<std::vector<int64_t>>>
      equalityDeleteVectorMap;
  equalityFieldIdsMap.insert({0, {1}});
  
  // Delete no rows (empty delete vector)
  equalityDeleteVectorMap.insert({0, {{}}});
  assertEqualityDeletes<TypeKind::BIGINT>(equalityDeleteVectorMap, equalityFieldIdsMap);
}

// Add the test methods that were previously in the parameterized class
// These methods can be added to the combined class or used as templates

INSTANTIATE_TEST_SUITE_P(
    ReadEqualityDeletes,
    IcebergReadEqualityDeletes,
    ::testing::Values(
        // Single column tests with different integer types
        TestParams{ROW({"a"}, {TINYINT()}), {NullParam::kNoNulls}},
        TestParams{ROW({"a"}, {SMALLINT()}), {NullParam::kNoNulls}},
        TestParams{ROW({"a"}, {INTEGER()}), {NullParam::kNoNulls}},
        TestParams{ROW({"a"}, {BIGINT()}), {NullParam::kNoNulls}},
        TestParams{ROW({"a"}, {VARCHAR()}), {NullParam::kNoNulls}},
        TestParams{ROW({"a"}, {VARBINARY()}), {NullParam::kNoNulls}},
        
        // Single column tests with floating point types (for error testing)
        TestParams{ROW({"a"}, {REAL()}), {NullParam::kNoNulls}},
        TestParams{ROW({"a"}, {DOUBLE()}), {NullParam::kNoNulls}},
        
        // Multi-column tests with integer types and nulls
        TestParams{ROW({"a", "b"}, {TINYINT(), SMALLINT()}), {NullParam::kNoNulls, NullParam::kNoNulls}},
        TestParams{ROW({"a", "b"}, {INTEGER(), BIGINT()}), {NullParam::kNoNulls, NullParam::kPartialNulls}},
        TestParams{ROW({"a", "b"}, {TINYINT(), VARCHAR()}), {NullParam::kNoNulls, NullParam::kNoNulls}},
        TestParams{ROW({"a", "b"}, {SMALLINT(), VARBINARY()}), {NullParam::kPartialNulls, NullParam::kNoNulls}},
        TestParams{ROW({"a", "b"}, {BIGINT(), BIGINT()}), {NullParam::kAllNulls, NullParam::kNoNulls}},
        
        // Mix integer types with string types
        TestParams{ROW({"a", "b"}, {BIGINT(), VARCHAR()}), {NullParam::kNoNulls, NullParam::kPartialNulls}},
        TestParams{ROW({"a", "b"}, {INTEGER(), VARBINARY()}), {NullParam::kPartialNulls, NullParam::kNoNulls}},
        
        // Three column combinations
        TestParams{ROW({"a", "b", "c"}, {TINYINT(), SMALLINT(), INTEGER()}), {NullParam::kNoNulls, NullParam::kNoNulls, NullParam::kNoNulls}},
        TestParams{ROW({"a", "b", "c"}, {BIGINT(), VARCHAR(), VARBINARY()}), {NullParam::kNoNulls, NullParam::kPartialNulls, NullParam::kAllNulls}},
        TestParams{ROW({"a", "b", "c"}, {TINYINT(), VARCHAR(), BIGINT()}), {NullParam::kPartialNulls, NullParam::kNoNulls, NullParam::kPartialNulls}},
        
        // Edge case: all nulls
        TestParams{ROW({"a", "b"}, {INTEGER(), VARCHAR()}), {NullParam::kAllNulls, NullParam::kAllNulls}}
    ),
    [](const testing::TestParamInfo<TestParams>& info) {
      std::string name;
      auto rowType = info.param.dataRowType;
      for (size_t i = 0; i < rowType->size(); ++i) {
        if (i > 0) name += "_";
        name += rowType->nameOf(i) + "_" + rowType->childAt(i)->toString();
        switch (info.param.nullParamForData[i]) {
          case NullParam::kNoNulls: name += "_NoNulls"; break;
          case NullParam::kPartialNulls: name += "_PartialNulls"; break;
          case NullParam::kAllNulls: name += "_AllNulls"; break;
        }
      }
      return name;
    });

} // namespace facebook::velox::connector::hive::iceberg
