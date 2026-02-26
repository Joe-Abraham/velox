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

#include "velox/connectors/hive/iceberg/IcebergDeleteFile.h"
#include "velox/connectors/hive/iceberg/IcebergMetadataColumns.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"
#include "velox/dwio/common/BufferUtil.h"

using namespace facebook::velox::dwio::common;

namespace facebook::velox::connector::hive::iceberg {

IcebergSplitReader::IcebergSplitReader(
    const std::shared_ptr<const hive::HiveConnectorSplit>& hiveSplit,
    const HiveTableHandlePtr& hiveTableHandle,
    const HiveColumnHandleMap* partitionKeys,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    const RowTypePtr& readerOutputType,
    const std::shared_ptr<io::IoStatistics>& ioStatistics,
    const std::shared_ptr<IoStats>& ioStats,
    FileHandleFactory* const fileHandleFactory,
    folly::Executor* executor,
    const std::shared_ptr<common::ScanSpec>& scanSpec)
    : SplitReader(
          hiveSplit,
          hiveTableHandle,
          partitionKeys,
          connectorQueryCtx,
          hiveConfig,
          readerOutputType,
          ioStatistics,
          ioStats,
          fileHandleFactory,
          executor,
          scanSpec),
      baseReadOffset_(0),
      splitOffset_(0),
      deleteBitmap_(nullptr) {}

void IcebergSplitReader::prepareSplit(
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    dwio::common::RuntimeStatistics& runtimeStats,
    const folly::F14FastMap<std::string, std::string>& fileReadOps) {
  createReader();
  if (emptySplit_) {
    return;
  }
  auto rowType = getAdaptedRowType();

  // Initialize row lineage tracking for _last_updated_sequence_number.
  dataSequenceNumber_ = std::nullopt;
  if (auto it = hiveSplit_->infoColumns.find("$data_sequence_number");
      it != hiveSplit_->infoColumns.end()) {
    dataSequenceNumber_ = folly::to<int64_t>(it->second);
  }
  readLastUpdatedSeqNumFromFile_ = false;
  lastUpdatedSeqNumOutputIndex_ = std::nullopt;
  if (dataSequenceNumber_.has_value()) {
    auto* seqNumSpec = scanSpec_->childByName(
        IcebergMetadataColumn::kLastUpdatedSequenceNumberColumnName);
    if (seqNumSpec && !seqNumSpec->isConstant()) {
      auto idx = readerOutputType_->getChildIdxIfExists(
          IcebergMetadataColumn::kLastUpdatedSequenceNumberColumnName);
      if (idx.has_value()) {
        readLastUpdatedSeqNumFromFile_ = true;
        lastUpdatedSeqNumOutputIndex_ = *idx;
      }
    }
  }

  if (checkIfSplitIsEmpty(runtimeStats)) {
    VELOX_CHECK(emptySplit_);
    return;
  }

  createRowReader(std::move(metadataFilter), std::move(rowType), std::nullopt);

  auto icebergSplit = checkedPointerCast<const HiveIcebergSplit>(hiveSplit_);
  baseReadOffset_ = 0;
  splitOffset_ = baseRowReader_->nextRowNumber();
  positionalDeleteFileReaders_.clear();

  const auto& deleteFiles = icebergSplit->deleteFiles;
  for (const auto& deleteFile : deleteFiles) {
    if (deleteFile.content == FileContent::kPositionalDeletes) {
      if (deleteFile.recordCount > 0) {
        // Skip the delete file if all delete positions are before this split.
        // TODO: Skip delete files where all positions are after the split, if
        // split row count becomes available.
        auto posColumn = IcebergMetadataColumn::icebergDeletePosColumn();
        if (auto posUpperBoundIt = deleteFile.upperBounds.find(posColumn->id);
            posUpperBoundIt != deleteFile.upperBounds.end()) {
          auto deleteUpperBound = folly::to<uint64_t>(posUpperBoundIt->second);
          if (deleteUpperBound < splitOffset_) {
            continue;
          }
        }
        positionalDeleteFileReaders_.push_back(
            std::make_unique<PositionalDeleteFileReader>(
                deleteFile,
                hiveSplit_->filePath,
                fileHandleFactory_,
                connectorQueryCtx_,
                ioExecutor_,
                hiveConfig_,
                ioStatistics_,
                ioStats_,
                runtimeStats,
                splitOffset_,
                hiveSplit_->connectorId));
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

  // For Iceberg V3 row lineage: replace 0 values in
  // _last_updated_sequence_number with the data sequence number from the
  // file's manifest entry. A value of 0 means the row has not been committed
  // to a table and must be inherited.
  if (readLastUpdatedSeqNumFromFile_ && dataSequenceNumber_.has_value() &&
      lastUpdatedSeqNumOutputIndex_.has_value() && rowsScanned > 0) {
    auto* rowOutput = output->as<RowVector>();
    if (rowOutput) {
      auto& seqNumChild =
          rowOutput->childAt(*lastUpdatedSeqNumOutputIndex_);
      if (seqNumChild->isConstantEncoding()) {
        auto* simpleVec = seqNumChild->as<SimpleVector<int64_t>>();
        if (simpleVec && !simpleVec->isNullAt(0) &&
            simpleVec->valueAt(0) == 0) {
          seqNumChild = std::make_shared<ConstantVector<int64_t>>(
              connectorQueryCtx_->memoryPool(),
              rowsScanned,
              false,
              BIGINT(),
              *dataSequenceNumber_);
        }
      } else if (auto* flatVec = seqNumChild->asFlatVector<int64_t>()) {
        for (vector_size_t i = 0; i < rowsScanned; ++i) {
          if (!flatVec->isNullAt(i) && flatVec->valueAt(i) == 0) {
            flatVec->set(i, *dataSequenceNumber_);
          }
        }
      }
    }
  }

  return rowsScanned;
}

std::vector<TypePtr> IcebergSplitReader::adaptColumns(
    const RowTypePtr& fileType,
    const RowTypePtr& tableSchema) const {
  // Resolve the data sequence number from split info columns for
  // _last_updated_sequence_number inheritance.
  std::optional<int64_t> dataSeqNum;
  if (auto it = hiveSplit_->infoColumns.find("$data_sequence_number");
      it != hiveSplit_->infoColumns.end()) {
    dataSeqNum = folly::to<int64_t>(it->second);
  }

  std::vector<TypePtr> columnTypes = fileType->children();
  auto& childrenSpecs = scanSpec_->children();
  // Iceberg table stores all column's data in data file.
  for (const auto& childSpec : childrenSpecs) {
    const std::string& fieldName = childSpec->fieldName();
    if (auto iter = hiveSplit_->infoColumns.find(fieldName);
        iter != hiveSplit_->infoColumns.end()) {
      auto infoColumnType = readerOutputType_->findChild(fieldName);
      auto constant = newConstantFromString(
          infoColumnType,
          iter->second,
          connectorQueryCtx_->memoryPool(),
          hiveConfig_->readTimestampPartitionValueAsLocalTime(
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
        if (auto it = hiveSplit_->partitionKeys.find(fieldName);
            it != hiveSplit_->partitionKeys.end()) {
          setPartitionValue(childSpec.get(), fieldName, it->second);
        } else if (
            fieldName ==
                IcebergMetadataColumn::
                    kLastUpdatedSequenceNumberColumnName &&
            dataSeqNum.has_value()) {
          childSpec->setConstantValue(
              std::make_shared<ConstantVector<int64_t>>(
                  connectorQueryCtx_->memoryPool(),
                  1,
                  false,
                  BIGINT(),
                  *dataSeqNum));
        } else {
          childSpec->setConstantValue(
              BaseVector::createNullConstant(
                  tableSchema->findChild(fieldName),
                  1,
                  connectorQueryCtx_->memoryPool()));
        }
      }
    }
  }

  scanSpec_->resetCachedValues(false);

  return columnTypes;
}

} // namespace facebook::velox::connector::hive::iceberg
