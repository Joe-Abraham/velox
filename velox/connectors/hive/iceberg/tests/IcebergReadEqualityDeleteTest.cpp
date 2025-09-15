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
#include "velox/connectors/hive/iceberg/tests/IcebergTestBase.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::velox::connector::hive::iceberg {

struct TestParams {
  std::vector<TypeKind> columnTypes;
  std::vector<NullParam> nullParamForData;
};

// Helper function to get a string representation of NullParam for test names
std::string nullParamToString(NullParam param) {
  switch (param) {
    case NullParam::kNoNulls:
      return "NoNulls";
    case NullParam::kPartialNulls:
      return "PartialNulls";
    case NullParam::kAllNulls:
      return "AllNulls";
    default:
      return "Unknown";
  }
}

// Helper function to get a string representation of TestParams for test names
std::string testParamsToString(const TestParams& params) {
  std::string result;
  for (size_t i = 0; i < params.columnTypes.size(); ++i) {
    result += mapTypeKindToName(params.columnTypes[i]) +
        nullParamToString(params.nullParamForData[i]);
  }
  return result;
}

class IcebergReadEqualityDeleteTest
    : public IcebergTestBase,
      public testing::WithParamInterface<TestParams> {
 private:
  std::shared_ptr<TempFilePath> writeEqualityDeleteFile(
      const std::vector<RowVectorPtr>& deleteVectors) {
    VELOX_CHECK_GT(deleteVectors.size(), 0);

    // Combine all delete vectors into one
    auto deleteFilePath = TempFilePath::create();
    writeToFile(deleteFilePath->getPath(), deleteVectors);

    return deleteFilePath;
  }

  std::string makeTypePredicate(
      const std::string& columnName,
      TypeKind columnType,
      const std::string& valueStr) {
    switch (columnType) {
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        return fmt::format(
            "({} IS NULL OR {} <> '{}')", columnName, columnName, valueStr);
      case TypeKind::TINYINT:
      case TypeKind::SMALLINT:
      case TypeKind::INTEGER:
      case TypeKind::BIGINT:
        return fmt::format(
            "({} IS NULL OR {} <> {})", columnName, columnName, valueStr);
      default:
        VELOX_FAIL(
            "Unsupported predicate type: {}", mapTypeKindToName(columnType));
    }
  }

  std::string makeTypePredicates(
      const std::vector<RowVectorPtr>& deleteVectors,
      const std::vector<int32_t>& equalityFieldIds,
      const std::vector<TypeKind>& columnTypes) {
    VELOX_CHECK_EQ(deleteVectors.size(), 1);
    VELOX_CHECK_EQ(equalityFieldIds.size(), columnTypes.size());

    if (deleteVectors.empty()) {
      return "";
    }

    // Get the number of delete rows from the first vector
    int32_t numDeletedRows = deleteVectors[0]->size();
    auto deleteRowVector = deleteVectors[0];

    std::string predicates;

    // Build predicates for each row to be deleted
    for (int32_t row = 0; row < numDeletedRows; ++row) {
      if (row > 0) {
        predicates += " AND ";
      }

      std::string rowPredicate = "(";

      // For each column in this delete row
      for (size_t i = 0; i < equalityFieldIds.size(); ++i) {
        if (i > 0) {
          rowPredicate += " AND ";
        }

        auto deleteVector = deleteRowVector->childAt(i);
        auto fieldId = equalityFieldIds[i];
        auto columnType = columnTypes[i];
        std::string columnName = fmt::format("c{}", fieldId - 1);

        // Check if the delete value is null
        if (deleteVector->isNullAt(row)) {
          // For null delete values, we exclude rows with null values
          rowPredicate += fmt::format("{} IS NOT NULL", columnName);
        } else {
          std::string valueStr;
          switch (columnType) {
            case TypeKind::TINYINT: {
              auto vector = deleteVector->as<FlatVector<int8_t>>();
              valueStr = std::to_string(vector->valueAt(row));
              break;
            }
            case TypeKind::SMALLINT: {
              auto vector = deleteVector->as<FlatVector<int16_t>>();
              valueStr = std::to_string(vector->valueAt(row));
              break;
            }
            case TypeKind::INTEGER: {
              auto vector = deleteVector->as<FlatVector<int32_t>>();
              valueStr = std::to_string(vector->valueAt(row));
              break;
            }
            case TypeKind::BIGINT: {
              auto vector = deleteVector->as<FlatVector<int64_t>>();
              valueStr = std::to_string(vector->valueAt(row));
              break;
            }
            case TypeKind::VARCHAR:
            case TypeKind::VARBINARY: {
              auto vector = deleteVector->as<FlatVector<StringView>>();
              valueStr = vector->valueAt(row).str();
              break;
            }
            default:
              VELOX_FAIL(
                  "Unsupported type for predicate: {}",
                  mapTypeKindToName(columnType));
          }
          rowPredicate += makeTypePredicate(columnName, columnType, valueStr);
        }
      }

      rowPredicate += ")";
      predicates += rowPredicate;
    }

    return predicates;
  }

 public:
  void assertEqualityDeletes(
      const std::vector<TypeKind>& columnTypes,
      const std::vector<NullParam>& nullParams,
      const std::vector<RowVectorPtr>& deleteVectors,
      const std::vector<int32_t>& equalityFieldIds,
      std::string duckDbSql = "") {
    folly::SingletonVault::singleton()->registrationComplete();

    std::vector<RowVectorPtr> dataVectors =
        makeVectors(1, rowCount_, columnTypes, nullParams);

    // Write data file
    auto dataFilePath = TempFilePath::create();
    writeToFile(dataFilePath->getPath(), dataVectors);

    // Create DuckDB table for comparison
    createDuckDbTable(dataVectors);

    // Write delete file
    auto deleteFilePath = writeEqualityDeleteFile(deleteVectors);

    // Create Iceberg delete file info
    std::vector<connector::hive::iceberg::IcebergDeleteFile> deleteFiles;
    int64_t deleteFileSize = 0;
    for (auto& deleteVec : deleteVectors) {
      deleteFileSize += deleteVec->size();
    }

    IcebergDeleteFile deleteFile(
        FileContent::kEqualityDeletes,
        deleteFilePath->getPath(),
        fileFormat_,
        deleteFileSize,
        testing::internal::GetFileSize(
            std::fopen(deleteFilePath->getPath().c_str(), "r")),
        equalityFieldIds);
    deleteFiles.push_back(deleteFile);

    auto icebergSplits =
        makeIcebergSplits(dataFilePath->getPath(), deleteFiles);

    // Generate DuckDB query
    if (duckDbSql == "") {
      duckDbSql = "SELECT * FROM tmp ";
      if (deleteFileSize > 0) {
        std::string predicates =
            makeTypePredicates(deleteVectors, equalityFieldIds, columnTypes);
        if (!predicates.empty()) {
          duckDbSql += fmt::format("WHERE {}", predicates);
        }
      }
    }

    assertQuery(
        icebergSplits.back(), asRowType(dataVectors[0]->type()), duckDbSql);
  }

  void assertQuery(
      std::shared_ptr<connector::ConnectorSplit> split,
      const RowTypePtr& outputRowType,
      const std::string& duckDbSql) {
    auto plan = tableScanNode(outputRowType);
    auto task = OperatorTestBase::assertQuery(plan, {split}, duckDbSql);

    auto planStats = toPlanStats(task->taskStats());
    auto scanNodeId = plan->id();
    auto it = planStats.find(scanNodeId);
    ASSERT_TRUE(it != planStats.end());
    ASSERT_TRUE(it->second.peakMemoryBytes > 0);
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
      dataVectors = makeVectorsImpl<KIND>(splitCount, numRows, numColumns);
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

  void testSubFieldEqualityDelete() {
    TestParams params = GetParam();

    // Skip floating point types for this test
    for (auto columnType : params.columnTypes) {
      if (columnType == TypeKind::REAL || columnType == TypeKind::DOUBLE) {
        GTEST_SKIP()
            << "Skipping floating point types for testSubFieldEqualityDelete";
      }
    }

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
              makeFlatVector<int64_t>(
                  20, [](auto row) { return row + 1; })})})};
    int32_t numDataColumns = 1;
    dataFilePath = writeDataFiles<TypeKind::BIGINT>(
        rowCount_, numDataColumns, 1, dataVectors)[0];

    // Write the delete file. Equality delete field is c_row.c1
    std::vector<IcebergDeleteFile> deleteFiles;
    // Delete rows {0, 1} from c_row.c1, whose schema ID is 4
    std::vector<RowVectorPtr> deleteDataVectors = {makeRowVector(
        {"c1"},
        {makeFlatVector<int64_t>(2, [](auto row) { return row + 1; })})};

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

    auto icebergSplits =
        makeIcebergSplits(dataFilePath->getPath(), deleteFiles);

    // Select both c_bigint and c_row column columns
    std::string duckDbSql = "SELECT * FROM tmp WHERE c_row.c0 not in (1, 2)";
    assertQuery(
        icebergSplits.back(), asRowType(dataVectors[0]->type()), duckDbSql);

    // SELECT only c_bigint column
    duckDbSql = "SELECT c_bigint FROM tmp WHERE c_row.c0 not in (1, 2)";
    assertQuery(icebergSplits.back(), ROW({"c_bigint"}, {BIGINT()}), duckDbSql);
  }

  void testFloatAndDoubleThrowsError() {
    TestParams params = GetParam();

    // Only test floating point types for this test
    bool hasFloatingPointTypes = false;
    TypeKind floatingPointType = TypeKind::REAL; // Default, will be updated

    for (auto columnType : params.columnTypes) {
      if (columnType == TypeKind::REAL || columnType == TypeKind::DOUBLE) {
        hasFloatingPointTypes = true;
        floatingPointType = columnType;
        break;
      }
    }

    if (!hasFloatingPointTypes) {
      GTEST_SKIP()
          << "Skipping non-floating point types for testFloatAndDoubleThrowsError";
      return;
    }

    // Create delete vectors using makeVectors
    std::vector<RowVectorPtr> deleteVectors =
        makeVectors(1, 2, params.columnTypes, params.nullParamForData);

    std::vector<int32_t> equalityFieldIds;
    for (size_t i = 0; i < params.columnTypes.size(); ++i) {
      equalityFieldIds.push_back(static_cast<int32_t>(i + 1));
    }

    std::string expectedErrorMessage;
    if (floatingPointType == TypeKind::REAL) {
      expectedErrorMessage =
          "Iceberg does not allow DOUBLE or REAL columns as the equality delete columns: c0 : REAL";
      VELOX_ASSERT_THROW(
          assertEqualityDeletes(
              params.columnTypes,
              params.nullParamForData,
              deleteVectors,
              equalityFieldIds),
          expectedErrorMessage);
    } else {
      expectedErrorMessage =
          "Iceberg does not allow DOUBLE or REAL columns as the equality delete columns: c0 : DOUBLE";
      VELOX_ASSERT_THROW(
          assertEqualityDeletes(
              params.columnTypes,
              params.nullParamForData,
              deleteVectors,
              equalityFieldIds),
          expectedErrorMessage);
    }
  }

  void testDeleteFirstAndLastRows() {
    TestParams params = GetParam();

    // Skip floating point types for this test
    for (auto columnType : params.columnTypes) {
      if (columnType == TypeKind::REAL || columnType == TypeKind::DOUBLE) {
        GTEST_SKIP()
            << "Skipping floating point types for testDeleteFirstAndLastRows";
      }
    }

    folly::SingletonVault::singleton()->registrationComplete();

    // Create test data using makeVectors
    std::vector<RowVectorPtr> dataVectors =
        makeVectors(1, rowCount_, params.columnTypes, params.nullParamForData);

    // Create delete vectors with first and last row values
    std::vector<VectorPtr> deleteVectorColumns;
    std::vector<std::string> columnNames;

    for (size_t i = 0; i < params.columnTypes.size(); ++i) {
      columnNames.push_back(fmt::format("c{}", i));
      auto dataColumn = dataVectors[0]->childAt(i);

      switch (params.columnTypes[i]) {
        case TypeKind::TINYINT: {
          auto flatVector = dataColumn->as<FlatVector<int8_t>>();
          std::vector<int8_t> deleteValues = {
              flatVector->valueAt(0), flatVector->valueAt(rowCount_ - 1)};
          deleteVectorColumns.push_back(makeFlatVector<int8_t>(deleteValues));
          break;
        }
        case TypeKind::SMALLINT: {
          auto flatVector = dataColumn->as<FlatVector<int16_t>>();
          std::vector<int16_t> deleteValues = {
              flatVector->valueAt(0), flatVector->valueAt(rowCount_ - 1)};
          deleteVectorColumns.push_back(makeFlatVector<int16_t>(deleteValues));
          break;
        }
        case TypeKind::INTEGER: {
          auto flatVector = dataColumn->as<FlatVector<int32_t>>();
          std::vector<int32_t> deleteValues = {
              flatVector->valueAt(0), flatVector->valueAt(rowCount_ - 1)};
          deleteVectorColumns.push_back(makeFlatVector<int32_t>(deleteValues));
          break;
        }
        case TypeKind::BIGINT: {
          auto flatVector = dataColumn->as<FlatVector<int64_t>>();
          std::vector<int64_t> deleteValues = {
              flatVector->valueAt(0), flatVector->valueAt(rowCount_ - 1)};
          deleteVectorColumns.push_back(makeFlatVector<int64_t>(deleteValues));
          break;
        }
        case TypeKind::VARCHAR:
        case TypeKind::VARBINARY: {
          auto flatVector = dataColumn->as<FlatVector<StringView>>();
          std::vector<StringView> deleteValues = {
              flatVector->valueAt(0), flatVector->valueAt(rowCount_ - 1)};
          deleteVectorColumns.push_back(
              makeFlatVector<StringView>(deleteValues));
          break;
        }
        default:
          VELOX_FAIL(
              "Unsupported type for testDeleteFirstAndLastRows: {}",
              mapTypeKindToName(params.columnTypes[i]));
      }
    }

    std::vector<RowVectorPtr> deleteVectors = {
        makeRowVector(columnNames, deleteVectorColumns)};

    // Create equality field IDs (all columns)
    std::vector<int32_t> equalityFieldIds;
    for (size_t i = 0; i < params.columnTypes.size(); ++i) {
      equalityFieldIds.push_back(static_cast<int32_t>(i + 1));
    }

    assertEqualityDeletes(
        params.columnTypes,
        params.nullParamForData,
        deleteVectors,
        equalityFieldIds);
  }

  void testDeleteRandomRows() {
    TestParams params = GetParam();

    // Skip floating point types for this test
    for (auto columnType : params.columnTypes) {
      if (columnType == TypeKind::REAL || columnType == TypeKind::DOUBLE) {
        GTEST_SKIP()
            << "Skipping floating point types for testDeleteRandomRows";
      }
    }

    folly::SingletonVault::singleton()->registrationComplete();

    // Create test data using makeVectors
    std::vector<RowVectorPtr> dataVectors =
        makeVectors(1, rowCount_, params.columnTypes, params.nullParamForData);

    // Generate random indices to delete
    auto randomIndices = makeRandomDeleteValues(rowCount_);

    // Create delete vectors with random row values
    std::vector<VectorPtr> deleteVectorColumns;
    std::vector<std::string> columnNames;

    for (size_t i = 0; i < params.columnTypes.size(); ++i) {
      columnNames.push_back(fmt::format("c{}", i));
      auto dataColumn = dataVectors[0]->childAt(i);

      switch (params.columnTypes[i]) {
        case TypeKind::TINYINT: {
          auto flatVector = dataColumn->as<FlatVector<int8_t>>();
          std::vector<int8_t> deleteValues;
          for (auto idx : randomIndices) {
            deleteValues.push_back(flatVector->valueAt(idx));
          }
          deleteVectorColumns.push_back(makeFlatVector<int8_t>(deleteValues));
          break;
        }
        case TypeKind::SMALLINT: {
          auto flatVector = dataColumn->as<FlatVector<int16_t>>();
          std::vector<int16_t> deleteValues;
          for (auto idx : randomIndices) {
            deleteValues.push_back(flatVector->valueAt(idx));
          }
          deleteVectorColumns.push_back(makeFlatVector<int16_t>(deleteValues));
          break;
        }
        case TypeKind::INTEGER: {
          auto flatVector = dataColumn->as<FlatVector<int32_t>>();
          std::vector<int32_t> deleteValues;
          for (auto idx : randomIndices) {
            deleteValues.push_back(flatVector->valueAt(idx));
          }
          deleteVectorColumns.push_back(makeFlatVector<int32_t>(deleteValues));
          break;
        }
        case TypeKind::BIGINT: {
          auto flatVector = dataColumn->as<FlatVector<int64_t>>();
          std::vector<int64_t> deleteValues;
          for (auto idx : randomIndices) {
            deleteValues.push_back(flatVector->valueAt(idx));
          }
          deleteVectorColumns.push_back(makeFlatVector<int64_t>(deleteValues));
          break;
        }
        case TypeKind::VARCHAR:
        case TypeKind::VARBINARY: {
          auto flatVector = dataColumn->as<FlatVector<StringView>>();
          std::vector<StringView> deleteValues;
          for (auto idx : randomIndices) {
            deleteValues.push_back(flatVector->valueAt(idx));
          }
          deleteVectorColumns.push_back(
              makeFlatVector<StringView>(deleteValues));
          break;
        }
        default:
          VELOX_FAIL(
              "Unsupported type for testDeleteRandomRows: {}",
              mapTypeKindToName(params.columnTypes[i]));
      }
    }

    std::vector<RowVectorPtr> deleteVectors = {
        makeRowVector(columnNames, deleteVectorColumns)};

    // Create equality field IDs (all columns)
    std::vector<int32_t> equalityFieldIds;
    for (size_t i = 0; i < params.columnTypes.size(); ++i) {
      equalityFieldIds.push_back(static_cast<int32_t>(i + 1));
    }

    assertEqualityDeletes(
        params.columnTypes,
        params.nullParamForData,
        deleteVectors,
        equalityFieldIds);
  }

  void testDeleteAllRows() {
    TestParams params = GetParam();

    // Skip floating point types for this test
    for (auto columnType : params.columnTypes) {
      if (columnType == TypeKind::REAL || columnType == TypeKind::DOUBLE) {
        GTEST_SKIP() << "Skipping floating point types for testDeleteAllRows";
      }
    }

    folly::SingletonVault::singleton()->registrationComplete();

    // Create test data using makeVectors
    std::vector<RowVectorPtr> dataVectors =
        makeVectors(1, rowCount_, params.columnTypes, params.nullParamForData);

    // Create delete vectors with all row values
    std::vector<VectorPtr> deleteVectorColumns;
    std::vector<std::string> columnNames;

    for (size_t i = 0; i < params.columnTypes.size(); ++i) {
      columnNames.push_back(fmt::format("c{}", i));
      auto dataColumn = dataVectors[0]->childAt(i);

      switch (params.columnTypes[i]) {
        case TypeKind::TINYINT: {
          auto flatVector = dataColumn->as<FlatVector<int8_t>>();
          std::vector<int8_t> deleteValues;
          deleteValues.reserve(rowCount_);
          for (int j = 0; j < rowCount_; ++j) {
            deleteValues.push_back(flatVector->valueAt(j));
          }
          deleteVectorColumns.push_back(makeFlatVector<int8_t>(deleteValues));
          break;
        }
        case TypeKind::SMALLINT: {
          auto flatVector = dataColumn->as<FlatVector<int16_t>>();
          std::vector<int16_t> deleteValues;
          deleteValues.reserve(rowCount_);
          for (int j = 0; j < rowCount_; ++j) {
            deleteValues.push_back(flatVector->valueAt(j));
          }
          deleteVectorColumns.push_back(makeFlatVector<int16_t>(deleteValues));
          break;
        }
        case TypeKind::INTEGER: {
          auto flatVector = dataColumn->as<FlatVector<int32_t>>();
          std::vector<int32_t> deleteValues;
          deleteValues.reserve(rowCount_);
          for (int j = 0; j < rowCount_; ++j) {
            deleteValues.push_back(flatVector->valueAt(j));
          }
          deleteVectorColumns.push_back(makeFlatVector<int32_t>(deleteValues));
          break;
        }
        case TypeKind::BIGINT: {
          auto flatVector = dataColumn->as<FlatVector<int64_t>>();
          std::vector<int64_t> deleteValues;
          deleteValues.reserve(rowCount_);
          for (int j = 0; j < rowCount_; ++j) {
            deleteValues.push_back(flatVector->valueAt(j));
          }
          deleteVectorColumns.push_back(makeFlatVector<int64_t>(deleteValues));
          break;
        }
        case TypeKind::VARCHAR:
        case TypeKind::VARBINARY: {
          auto flatVector = dataColumn->as<FlatVector<StringView>>();
          std::vector<StringView> deleteValues;
          deleteValues.reserve(rowCount_);
          for (int j = 0; j < rowCount_; ++j) {
            deleteValues.push_back(flatVector->valueAt(j));
          }
          deleteVectorColumns.push_back(
              makeFlatVector<StringView>(deleteValues));
          break;
        }
        default:
          VELOX_FAIL(
              "Unsupported type for testDeleteAllRows: {}",
              mapTypeKindToName(params.columnTypes[i]));
      }
    }

    std::vector<RowVectorPtr> deleteVectors = {
        makeRowVector(columnNames, deleteVectorColumns)};

    // Create equality field IDs (all columns)
    std::vector<int32_t> equalityFieldIds;
    for (size_t i = 0; i < params.columnTypes.size(); ++i) {
      equalityFieldIds.push_back(static_cast<int32_t>(i + 1));
    }

    assertEqualityDeletes(
        params.columnTypes,
        params.nullParamForData,
        deleteVectors,
        equalityFieldIds);
  }

  void testDeleteNoRows() {
    TestParams params = GetParam();

    // Skip floating point types for this test
    for (auto columnType : params.columnTypes) {
      if (columnType == TypeKind::REAL || columnType == TypeKind::DOUBLE) {
        GTEST_SKIP() << "Skipping floating point types for testDeleteNoRows";
        return;
      }
    }

    folly::SingletonVault::singleton()->registrationComplete();

    // Create empty delete vectors (no rows to delete)
    std::vector<VectorPtr> deleteVectorColumns;
    std::vector<std::string> columnNames;

    for (size_t i = 0; i < params.columnTypes.size(); ++i) {
      columnNames.push_back(fmt::format("c{}", i));

      switch (params.columnTypes[i]) {
        case TypeKind::TINYINT:
          deleteVectorColumns.push_back(
              makeFlatVector<int8_t>(std::vector<int8_t>{}));
          break;
        case TypeKind::SMALLINT:
          deleteVectorColumns.push_back(
              makeFlatVector<int16_t>(std::vector<int16_t>{}));
          break;
        case TypeKind::INTEGER:
          deleteVectorColumns.push_back(
              makeFlatVector<int32_t>(std::vector<int32_t>{}));
          break;
        case TypeKind::BIGINT:
          deleteVectorColumns.push_back(
              makeFlatVector<int64_t>(std::vector<int64_t>{}));
          break;
        case TypeKind::VARCHAR:
        case TypeKind::VARBINARY:
          deleteVectorColumns.push_back(
              makeFlatVector<StringView>(std::vector<StringView>{}));
          break;
        default:
          VELOX_FAIL(
              "Unsupported type for testDeleteNoRows: {}",
              mapTypeKindToName(params.columnTypes[i]));
      }
    }

    std::vector<RowVectorPtr> deleteVectors = {
        makeRowVector(columnNames, deleteVectorColumns)};

    // Create equality field IDs (all columns)
    std::vector<int32_t> equalityFieldIds;
    for (size_t i = 0; i < params.columnTypes.size(); ++i) {
      equalityFieldIds.push_back(static_cast<int32_t>(i + 1));
    }

    assertEqualityDeletes(
        params.columnTypes,
        params.nullParamForData,
        deleteVectors,
        equalityFieldIds);
  }
};

TEST_P(IcebergReadEqualityDeleteTest, testSubFieldEqualityDelete) {
  testSubFieldEqualityDelete();
}

TEST_P(IcebergReadEqualityDeleteTest, floatAndDoubleThrowsError) {
  testFloatAndDoubleThrowsError();
}

TEST_P(IcebergReadEqualityDeleteTest, deleteFirstAndLastRows) {
  testDeleteFirstAndLastRows();
}

TEST_P(IcebergReadEqualityDeleteTest, deleteRandomRows) {
  testDeleteRandomRows();
}

TEST_P(IcebergReadEqualityDeleteTest, deleteAllRows) {
  testDeleteAllRows();
}

TEST_P(IcebergReadEqualityDeleteTest, deleteNoRows) {
  testDeleteNoRows();
}

INSTANTIATE_TEST_SUITE_P(
    AllTypes,
    IcebergReadEqualityDeleteTest,
    testing::Values(
        // Single row tests - No nulls
        TestParams{{TypeKind::TINYINT}, {NullParam::kNoNulls}},
        TestParams{{TypeKind::SMALLINT}, {NullParam::kNoNulls}},
        TestParams{{TypeKind::INTEGER}, {NullParam::kNoNulls}},
        TestParams{{TypeKind::BIGINT}, {NullParam::kNoNulls}},
        TestParams{{TypeKind::VARCHAR}, {NullParam::kNoNulls}},
        TestParams{{TypeKind::VARBINARY}, {NullParam::kNoNulls}},

        // Single row tests - Partial nulls
        TestParams{{TypeKind::TINYINT}, {NullParam::kPartialNulls}},
        TestParams{{TypeKind::SMALLINT}, {NullParam::kPartialNulls}},
        TestParams{{TypeKind::INTEGER}, {NullParam::kPartialNulls}},
        TestParams{{TypeKind::BIGINT}, {NullParam::kPartialNulls}},
        TestParams{{TypeKind::VARCHAR}, {NullParam::kPartialNulls}},
        TestParams{{TypeKind::VARBINARY}, {NullParam::kPartialNulls}},

        // Single row tests - All nulls
        TestParams{{TypeKind::TINYINT}, {NullParam::kAllNulls}},
        TestParams{{TypeKind::SMALLINT}, {NullParam::kAllNulls}},
        TestParams{{TypeKind::INTEGER}, {NullParam::kAllNulls}},
        TestParams{{TypeKind::BIGINT}, {NullParam::kAllNulls}},
        TestParams{{TypeKind::VARCHAR}, {NullParam::kAllNulls}},
        TestParams{{TypeKind::VARBINARY}, {NullParam::kAllNulls}},

        // Test for float and real - for floatAndDoubleThrowsError test
        TestParams{{TypeKind::REAL}, {NullParam::kNoNulls}},
        TestParams{{TypeKind::DOUBLE}, {NullParam::kNoNulls}},

        // Multiple row tests - No nulls (two same-type columns)
        TestParams{
            {TypeKind::TINYINT, TypeKind::TINYINT},
            {NullParam::kNoNulls, NullParam::kNoNulls}},
        TestParams{
            {TypeKind::SMALLINT, TypeKind::SMALLINT},
            {NullParam::kNoNulls, NullParam::kNoNulls}},
        TestParams{
            {TypeKind::INTEGER, TypeKind::INTEGER},
            {NullParam::kNoNulls, NullParam::kNoNulls}},
        TestParams{
            {TypeKind::BIGINT, TypeKind::BIGINT},
            {NullParam::kNoNulls, NullParam::kNoNulls}},
        TestParams{
            {TypeKind::VARCHAR, TypeKind::VARCHAR},
            {NullParam::kNoNulls, NullParam::kNoNulls}},
        TestParams{
            {TypeKind::VARBINARY, TypeKind::VARBINARY},
            {NullParam::kNoNulls, NullParam::kNoNulls}},

        // Multiple row tests - Partial nulls (two same-type columns)
        TestParams{
            {TypeKind::TINYINT, TypeKind::TINYINT},
            {NullParam::kPartialNulls, NullParam::kPartialNulls}},
        TestParams{
            {TypeKind::SMALLINT, TypeKind::SMALLINT},
            {NullParam::kPartialNulls, NullParam::kPartialNulls}},
        TestParams{
            {TypeKind::INTEGER, TypeKind::INTEGER},
            {NullParam::kPartialNulls, NullParam::kPartialNulls}},
        TestParams{
            {TypeKind::BIGINT, TypeKind::BIGINT},
            {NullParam::kPartialNulls, NullParam::kPartialNulls}},
        TestParams{
            {TypeKind::VARCHAR, TypeKind::VARCHAR},
            {NullParam::kPartialNulls, NullParam::kPartialNulls}},
        TestParams{
            {TypeKind::VARBINARY, TypeKind::VARBINARY},
            {NullParam::kPartialNulls, NullParam::kPartialNulls}},

        // Multiple row tests - All nulls (two same-type columns)
        TestParams{
            {TypeKind::TINYINT, TypeKind::TINYINT},
            {NullParam::kAllNulls, NullParam::kAllNulls}},
        TestParams{
            {TypeKind::SMALLINT, TypeKind::SMALLINT},
            {NullParam::kAllNulls, NullParam::kAllNulls}},
        TestParams{
            {TypeKind::INTEGER, TypeKind::INTEGER},
            {NullParam::kAllNulls, NullParam::kAllNulls}},
        TestParams{
            {TypeKind::BIGINT, TypeKind::BIGINT},
            {NullParam::kAllNulls, NullParam::kAllNulls}},
        TestParams{
            {TypeKind::VARCHAR, TypeKind::VARCHAR},
            {NullParam::kAllNulls, NullParam::kAllNulls}},
        TestParams{
            {TypeKind::VARBINARY, TypeKind::VARBINARY},
            {NullParam::kAllNulls, NullParam::kAllNulls}},

        // Mixed type tests (from former MixedTypeTestParams)
        // Two column tests with the same types but different nulls
        TestParams{
            {TypeKind::BIGINT, TypeKind::BIGINT},
            {NullParam::kNoNulls, NullParam::kPartialNulls}},
        // Mixed type tests with TINYINT and VARCHAR
        TestParams{
            {TypeKind::TINYINT, TypeKind::VARCHAR},
            {NullParam::kNoNulls, NullParam::kNoNulls}},
        TestParams{
            {TypeKind::TINYINT, TypeKind::VARCHAR},
            {NullParam::kPartialNulls, NullParam::kPartialNulls}},
        // Mixed type tests with INTEGER and VARCHAR
        TestParams{
            {TypeKind::INTEGER, TypeKind::VARCHAR},
            {NullParam::kNoNulls, NullParam::kNoNulls}},
        TestParams{
            {TypeKind::INTEGER, TypeKind::VARCHAR},
            {NullParam::kPartialNulls, NullParam::kNoNulls}},
        TestParams{
            {TypeKind::INTEGER, TypeKind::VARCHAR},
            {NullParam::kNoNulls, NullParam::kPartialNulls}},
        TestParams{
            {TypeKind::INTEGER, TypeKind::VARCHAR},
            {NullParam::kPartialNulls, NullParam::kPartialNulls}},
        // Mixed type tests with SMALLINT and VARBINARY
        TestParams{
            {TypeKind::SMALLINT, TypeKind::VARBINARY},
            {NullParam::kNoNulls, NullParam::kNoNulls}},
        TestParams{
            {TypeKind::SMALLINT, TypeKind::VARBINARY},
            {NullParam::kPartialNulls, NullParam::kPartialNulls}},
        // Three column mixed type tests
        TestParams{
            {TypeKind::INTEGER, TypeKind::VARCHAR, TypeKind::BIGINT},
            {NullParam::kNoNulls,
             NullParam::kPartialNulls,
             NullParam::kNoNulls}}),
    [](const testing::TestParamInfo<TestParams>& info) {
      return testParamsToString(info.param);
    });

} // namespace facebook::velox::connector::hive::iceberg
