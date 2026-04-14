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

#include "velox/connectors/hive/iceberg/IcebergSplitReader.h"

#include <folly/lang/Bits.h>

#include "velox/common/encode/Base64.h"
#include "velox/connectors/hive/iceberg/IcebergDeleteFile.h"
#include "velox/connectors/hive/iceberg/IcebergMetadataColumns.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"
#include "velox/dwio/common/BufferUtil.h"
#include "velox/vector/DecodedVector.h"

using namespace facebook::velox::dwio::common;

namespace {

// Fills null slots in a BIGINT child vector with values produced by valueAt(i),
// where i is the output row index. If the whole vector is a null constant,
// replaces it with a flat vector. If only some slots are null, copies non-null
// values and fills null slots with valueAt(i). No-op when no nulls are present.
template <typename F>
void fillNullsWithInt64(
    facebook::velox::VectorPtr& child,
    facebook::velox::memory::MemoryPool* pool,
    F valueAt) {
  using namespace facebook::velox;
  child = BaseVector::loadedVectorShared(child);
  const auto vectorSize = child->size();
  if (vectorSize == 0) {
    return;
  }
  if (child->isConstantEncoding() && child->isNullAt(0)) {
    auto flat =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), vectorSize, pool);
    for (vector_size_t i = 0; i < vectorSize; ++i) {
      flat->set(i, valueAt(i));
    }
    child = std::move(flat);
  } else if (child->mayHaveNulls()) {
    const DecodedVector decoded(*child);
    if (decoded.mayHaveNulls()) {
      auto flat =
          BaseVector::create<FlatVector<int64_t>>(BIGINT(), vectorSize, pool);
      for (vector_size_t i = 0; i < vectorSize; ++i) {
        if (decoded.isNullAt(i)) {
          flat->set(i, valueAt(i));
        } else {
          flat->set(i, decoded.valueAt<int64_t>(i));
        }
      }
      child = std::move(flat);
    }
  }
}

} // namespace

namespace facebook::velox::connector::hive::iceberg {

IcebergSplitReader::IcebergSplitReader(
    const std::shared_ptr<const HiveIcebergSplit>& icebergSplit,
    const FileTableHandlePtr& tableHandle,
    const std::unordered_map<std::string, FileColumnHandlePtr>* partitionKeys,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<const FileConfig>& fileConfig,
    const RowTypePtr& readerOutputType,
    const std::shared_ptr<io::IoStatistics>& ioStatistics,
    const std::shared_ptr<IoStats>& ioStats,
    FileHandleFactory* const fileHandleFactory,
    folly::Executor* executor,
    const std::shared_ptr<common::ScanSpec>& scanSpec)
    : FileSplitReader(
          icebergSplit,
          tableHandle,
          partitionKeys,
          connectorQueryCtx,
          fileConfig,
          readerOutputType,
          ioStatistics,
          ioStats,
          fileHandleFactory,
          executor,
          scanSpec),
      icebergSplit_(icebergSplit),
      baseReadOffset_(0),
      splitOffset_(0),
      deleteBitmap_(nullptr) {}

void IcebergSplitReader::prepareSplit(
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    dwio::common::RuntimeStatistics& runtimeStats,
    const folly::F14FastMap<std::string, std::string>& fileReadOps) {
  // Temporarily extend the file schema with projected row-lineage columns
  // (_row_id, _last_updated_sequence_number) so the reader allocates output
  // slots for them. Restored after createReader() to avoid persisting across
  // splits.
  auto originalFileSchema = baseReaderOpts_.fileSchema();
  if (originalFileSchema) {
    auto names = originalFileSchema->names();
    auto types = originalFileSchema->children();
    bool modified = false;
    for (const auto* colName :
         {IcebergMetadataColumn::kRowIdColumnName,
          IcebergMetadataColumn::kLastUpdatedSequenceNumberColumnName}) {
      if (readerOutputType_->containsChild(colName) &&
          !originalFileSchema->containsChild(colName)) {
        names.push_back(std::string(colName));
        types.push_back(BIGINT());
        modified = true;
      }
    }
    if (modified) {
      baseReaderOpts_.setFileSchema(ROW(std::move(names), std::move(types)));
    }
  }

  createReader(fileReadOps);

  if (originalFileSchema) {
    baseReaderOpts_.setFileSchema(originalFileSchema);
  }
  if (emptySplit_) {
    return;
  }

  firstRowId_ = std::nullopt;
  if (auto it = icebergSplit_->infoColumns.find(
          IcebergMetadataColumn::kFirstRowIdInfoColumn);
      it != icebergSplit_->infoColumns.end()) {
    auto value = folly::to<int64_t>(it->second);
    VELOX_CHECK_GE(value, 0, "First row ID must be non-negative");
    firstRowId_ = value;
  }

  dataSequenceNumber_ = std::nullopt;
  if (auto it = icebergSplit_->infoColumns.find(
          IcebergMetadataColumn::kDataSequenceNumberInfoColumn);
      it != icebergSplit_->infoColumns.end()) {
    dataSequenceNumber_ = folly::to<int64_t>(it->second);
  }

  // getAdaptedRowType() calls adaptColumns(), which may set
  // _last_updated_sequence_number to a constant. Must run after
  // dataSequenceNumber_ and firstRowId_ are initialized.
  auto rowType = getAdaptedRowType();

  lastUpdatedSeqNumOutputIndex_ = std::nullopt;
  if (dataSequenceNumber_.has_value()) {
    auto* seqNumSpec = scanSpec_->childByName(
        IcebergMetadataColumn::kLastUpdatedSequenceNumberColumnName);
    if (seqNumSpec && !seqNumSpec->isConstant()) {
      lastUpdatedSeqNumOutputIndex_ = readerOutputType_->getChildIdxIfExists(
          IcebergMetadataColumn::kLastUpdatedSequenceNumberColumnName);
    }
  }

  rowIdOutputIndex_ = std::nullopt;
  if (firstRowId_.has_value()) {
    rowIdOutputIndex_ = readerOutputType_->getChildIdxIfExists(
        IcebergMetadataColumn::kRowIdColumnName);
  }

  if (checkIfSplitIsEmpty(runtimeStats)) {
    VELOX_CHECK(emptySplit_);
    return;
  }

  // Inject a row-number column when filters, random-skip, or positional
  // deletes make the output-to-file-position mapping non-contiguous.
  // Check split metadata rather than positionalDeleteFileReaders_ because
  // the row reader must be configured before delete files are opened.
  const bool hasPositionalDeletes = std::any_of(
      icebergSplit_->deleteFiles.begin(),
      icebergSplit_->deleteFiles.end(),
      [](const IcebergDeleteFile& deleteFile) {
        return deleteFile.content == FileContent::kPositionalDeletes &&
            deleteFile.recordCount > 0;
      });
  useRowNumberColumn_ = rowIdOutputIndex_.has_value() &&
      (scanSpec_->hasFilter() || baseReaderOpts_.randomSkip() != nullptr ||
       hasPositionalDeletes);
  if (useRowNumberColumn_) {
    dwio::common::RowNumberColumnInfo rowNumInfo;
    rowNumInfo.insertPosition = readerOutputType_->size();
    rowNumInfo.name = "";
    baseRowReaderOpts_.setRowNumberColumnInfo(rowNumInfo);
  } else {
    baseRowReaderOpts_.setRowNumberColumnInfo(std::nullopt);
  }

  createRowReader(std::move(metadataFilter), std::move(rowType), std::nullopt);

  baseReadOffset_ = 0;
  splitOffset_ = baseRowReader_->nextRowNumber();
  positionalDeleteFileReaders_.clear();

  const auto& deleteFiles = icebergSplit_->deleteFiles;
  for (const auto& deleteFile : deleteFiles) {
    if (deleteFile.content == FileContent::kPositionalDeletes) {
      if (deleteFile.recordCount > 0) {
        // Skip the delete file if all delete positions are before this split.
        // TODO: Skip delete files where all positions are after the split, if
        // split row count becomes available.
        if (auto iter =
                deleteFile.upperBounds.find(IcebergMetadataColumn::kPosId);
            iter != deleteFile.upperBounds.end()) {
          auto decodedBound = encoding::Base64::decode(iter->second);
          VELOX_CHECK_EQ(
              decodedBound.size(),
              sizeof(uint64_t),
              "Unexpected decoded size for positional delete upper bound.");
          uint64_t posDeleteUpperBound;
          std::memcpy(
              &posDeleteUpperBound, decodedBound.data(), sizeof(uint64_t));
          posDeleteUpperBound = folly::Endian::little(posDeleteUpperBound);
          if (posDeleteUpperBound < splitOffset_) {
            continue;
          }
        }
        positionalDeleteFileReaders_.push_back(
            std::make_unique<PositionalDeleteFileReader>(
                deleteFile,
                fileSplit_->filePath,
                fileHandleFactory_,
                connectorQueryCtx_,
                ioExecutor_,
                fileConfig_,
                ioStatistics_,
                ioStats_,
                runtimeStats,
                splitOffset_,
                fileSplit_->connectorId));
      }
    } else {
      VELOX_NYI();
    }
  }
}

uint64_t IcebergSplitReader::next(uint64_t size, VectorPtr& output) {
  Mutation mutation;
  mutation.randomSkip = baseReaderOpts_.randomSkip().get();
  mutation.deletedRows = nullptr;

  if (deleteBitmap_) {
    std::memset(
        (void*)(deleteBitmap_->asMutable<int8_t>()), 0L, deleteBitmap_->size());
  }

  const auto actualSize = baseRowReader_->nextReadSize(size);
  baseReadOffset_ = baseRowReader_->nextRowNumber() - splitOffset_;
  if (actualSize == dwio::common::RowReader::kAtEnd) {
    return 0;
  }

  if (!positionalDeleteFileReaders_.empty()) {
    auto numBytes = bits::nbytes(actualSize);
    dwio::common::ensureCapacity<int8_t>(
        deleteBitmap_, numBytes, connectorQueryCtx_->memoryPool(), false, true);

    for (auto iter = positionalDeleteFileReaders_.begin();
         iter != positionalDeleteFileReaders_.end();) {
      (*iter)->readDeletePositions(baseReadOffset_, actualSize, deleteBitmap_);

      if ((*iter)->noMoreData()) {
        iter = positionalDeleteFileReaders_.erase(iter);
      } else {
        ++iter;
      }
    }
  }

  mutation.deletedRows = deleteBitmap_ && deleteBitmap_->size() > 0
      ? deleteBitmap_->as<uint64_t>()
      : nullptr;

  auto rowsScanned = baseRowReader_->next(actualSize, output, &mutation);

  auto* pool = connectorQueryCtx_->memoryPool();

  if (lastUpdatedSeqNumOutputIndex_.has_value() &&
      dataSequenceNumber_.has_value() && rowsScanned > 0) {
    if (auto* rowOutput = output->as<RowVector>()) {
      auto& seqNumChild = rowOutput->childAt(*lastUpdatedSeqNumOutputIndex_);
      const int64_t seqNum = static_cast<int64_t>(*dataSequenceNumber_);
      fillNullsWithInt64(
          seqNumChild, pool, [seqNum](vector_size_t) { return seqNum; });
    }
  }

  if (rowIdOutputIndex_.has_value() && firstRowId_.has_value() &&
      rowsScanned > 0) {
    if (auto* rowOutput = output->as<RowVector>()) {
      auto& rowIdChild = rowOutput->childAt(*rowIdOutputIndex_);
      if (useRowNumberColumn_) {
        // Use the injected row-number column for file-absolute positions.
        const column_index_t rowNumIdx =
            baseRowReaderOpts_.rowNumberColumnInfo()->insertPosition;
        const DecodedVector decodedRowNums(*rowOutput->childAt(rowNumIdx));
        const int64_t firstRowId = static_cast<int64_t>(*firstRowId_);
        fillNullsWithInt64(rowIdChild, pool, [&](vector_size_t i) {
          return firstRowId + decodedRowNums.valueAt<int64_t>(i);
        });

        // Strip the injected row-number column from the output.
        auto& rowType = rowOutput->type()->asRow();
        auto names = rowType.names();
        auto types = rowType.children();
        auto children = rowOutput->children();
        names.erase(names.begin() + rowNumIdx);
        types.erase(types.begin() + rowNumIdx);
        children.erase(children.begin() + rowNumIdx);
        output = std::make_shared<RowVector>(
            rowOutput->pool(),
            ROW(std::move(names), std::move(types)),
            rowOutput->nulls(),
            rowOutput->size(),
            std::move(children));
      } else {
        // Contiguous output: _row_id = firstRowId_ + file_pos.
        const int64_t base = static_cast<int64_t>(*firstRowId_) +
            static_cast<int64_t>(splitOffset_ + baseReadOffset_);
        fillNullsWithInt64(rowIdChild, pool, [base](vector_size_t i) {
          return base + static_cast<int64_t>(i);
        });
      }
    }
  }

  return rowsScanned;
}

std::vector<TypePtr> IcebergSplitReader::adaptColumns(
    const RowTypePtr& fileType,
    const RowTypePtr& tableSchema) const {
  std::vector<TypePtr> columnTypes = fileType->children();
  auto& childrenSpecs = scanSpec_->children();
  const auto& splitInfoColumns = icebergSplit_->infoColumns;
  // Iceberg table stores all column's data in data file.
  for (const auto& childSpec : childrenSpecs) {
    const std::string& fieldName = childSpec->fieldName();
    if (auto iter = splitInfoColumns.find(fieldName);
        iter != splitInfoColumns.end()) {
      auto infoColumnType = readerOutputType_->findChild(fieldName);
      auto constant = newConstantFromString(
          infoColumnType,
          iter->second,
          connectorQueryCtx_->memoryPool(),
          fileConfig_->readTimestampPartitionValueAsLocalTime(
              connectorQueryCtx_->sessionProperties()),
          false);
      childSpec->setConstantValue(constant);
    } else {
      auto fileTypeIdx = fileType->getChildIdxIfExists(fieldName);
      auto outputTypeIdx = readerOutputType_->getChildIdxIfExists(fieldName);
      if (outputTypeIdx.has_value() && fileTypeIdx.has_value()) {
        childSpec->setConstantValue(nullptr);
        auto& outputType = readerOutputType_->childAt(*outputTypeIdx);
        columnTypes[*fileTypeIdx] = outputType;
      } else if (!fileTypeIdx.has_value()) {
        // Handle columns missing from the data file in two scenarios:
        // 1. Schema evolution: Column was added after the data file was
        // written and doesn't exist in older data files.
        // 2. Partition columns: Hive migrated table. In Hive-written data
        // files, partition column values are stored in partition metadata
        // rather than in the data file itself, following Hive's partitioning
        // convention.
        // 3. _last_updated_sequence_number: For Iceberg V3 row lineage, if
        // the column is not in the file, inherit the data sequence number
        // from the file's manifest entry.
        // 4. _row_id: For Iceberg V3 row lineage, if the column is not in
        // the file, set as NULL constant here. When first_row_id is
        // available, next() will replace NULL with first_row_id + file
        // position.
        if (auto it = fileSplit_->partitionKeys.find(fieldName);
            it != fileSplit_->partitionKeys.end()) {
          setPartitionValue(childSpec.get(), fieldName, it->second);
        } else if (
            fieldName ==
                IcebergMetadataColumn::kLastUpdatedSequenceNumberColumnName &&
            dataSequenceNumber_.has_value()) {
          childSpec->setConstantValue(
              std::make_shared<ConstantVector<int64_t>>(
                  connectorQueryCtx_->memoryPool(),
                  1,
                  false,
                  BIGINT(),
                  static_cast<int64_t>(*dataSequenceNumber_)));
        } else {
          auto outputIdx = readerOutputType_->getChildIdxIfExists(fieldName);
          TypePtr colType;
          if (outputIdx.has_value()) {
            colType = readerOutputType_->childAt(*outputIdx);
          } else {
            VELOX_CHECK_NOT_NULL(
                tableSchema,
                "Table schema is required to resolve type for column not in output: {}",
                fieldName);
            colType = tableSchema->findChild(fieldName);
          }
          childSpec->setConstantValue(
              BaseVector::createNullConstant(
                  colType, 1, connectorQueryCtx_->memoryPool()));
        }
      }
    }
  }

  scanSpec_->resetCachedValues(false);

  return columnTypes;
}

} // namespace facebook::velox::connector::hive::iceberg
