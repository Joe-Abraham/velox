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

#include "velox/connectors/hive/iceberg/IcebergDataSource.h"

#include <algorithm>

#include "velox/connectors/hive/iceberg/IcebergSplit.h"
#include "velox/connectors/hive/iceberg/IcebergSplitReader.h"

namespace facebook::velox::connector::hive::iceberg {

namespace {

// A filter on an unprojected column resolves its type against
// 'dataColumns' (see HiveConnectorUtil::makeScanSpec()). Row-lineage
// columns (_row_id, _last_updated_sequence_number) and schema-evolution
// default columns are never physically stored, so they're missing from
// 'dataColumns', and such a filter throws "Field not found" before a split
// reader exists. Appends each unresolved filter-only handle's type to
// 'dataColumns'; appending (not inserting) preserves existing field-id
// indexing. No-op if 'tableHandle' isn't a HiveTableHandle, has no
// dataColumns, or every filter-only handle already resolves.
ConnectorTableHandlePtr addFilterOnlyColumnsToDataColumns(
    const ConnectorTableHandlePtr& tableHandle) {
  auto* hiveTableHandle =
      dynamic_cast<const HiveTableHandle*>(tableHandle.get());
  if (hiveTableHandle == nullptr) {
    return tableHandle;
  }

  const auto& dataColumns = hiveTableHandle->dataColumns();
  if (dataColumns == nullptr) {
    return tableHandle;
  }

  auto names = dataColumns->names();
  auto types = dataColumns->children();
  bool modified = false;
  for (const auto& handle : hiveTableHandle->hiveFilterColumnHandles()) {
    if (!dataColumns->containsChild(handle->name()) &&
        std::find(names.begin(), names.end(), handle->name()) == names.end()) {
      names.push_back(handle->name());
      types.push_back(handle->hiveType());
      modified = true;
    }
  }
  if (!modified) {
    return tableHandle;
  }

  common::SubfieldFilters subfieldFilters;
  for (const auto& [subfield, filter] : hiveTableHandle->subfieldFilters()) {
    subfieldFilters.emplace(subfield.clone(), filter);
  }

  return std::make_shared<HiveTableHandle>(
      hiveTableHandle->connectorId(),
      hiveTableHandle->tableName(),
      std::move(subfieldFilters),
      hiveTableHandle->remainingFilter(),
      ROW(std::move(names), std::move(types)),
      hiveTableHandle->indexColumns(),
      hiveTableHandle->tableParameters(),
      hiveTableHandle->hiveFilterColumnHandles(),
      hiveTableHandle->sampleRate(),
      hiveTableHandle->dbName());
}

// IcebergSplitReader::adaptColumns() looks up a missing column's default
// value via 'columnHandles_', which is built from 'assignments' and covers
// only projected columns. A filter-only column's default lives in
// filterColumnHandles() instead, so add those too, keyed by the handle's
// own name (filter-only columns have no output alias). 'tableHandle' is
// guaranteed castable to FileTableHandle by FileDataSource's constructor,
// which runs before this member initializes.
std::shared_ptr<ColumnHandleMap> makeColumnHandles(
    const ColumnHandleMap& assignments,
    const ConnectorTableHandlePtr& tableHandle) {
  auto columnHandles = std::make_shared<ColumnHandleMap>(assignments);
  for (const auto& handle :
       checkedPointerCast<const FileTableHandle>(tableHandle)
           ->filterColumnHandles()) {
    columnHandles->emplace(handle->name(), handle);
  }
  return columnHandles;
}

} // namespace

IcebergDataSource::IcebergDataSource(
    const RowTypePtr& outputType,
    const ConnectorTableHandlePtr& tableHandle,
    const ColumnHandleMap& assignments,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* ioExecutor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<HiveConfig>& hiveConfig)
    : HiveDataSource(
          outputType,
          addFilterOnlyColumnsToDataColumns(tableHandle),
          assignments,
          fileHandleFactory,
          ioExecutor,
          connectorQueryCtx,
          hiveConfig),
      columnHandles_(makeColumnHandles(assignments, tableHandle)) {}

std::unique_ptr<FileSplitReader> IcebergDataSource::createSplitReader() {
  prepareSplit();
  auto icebergSplit = checkedPointerCast<const HiveIcebergSplit>(split_);

  auto reader = std::make_unique<IcebergSplitReader>(
      icebergSplit,
      tableHandle_,
      &partitionKeys_,
      connectorQueryCtx_,
      fileConfig_,
      readerOutputType_,
      dataIoStats_,
      metadataIoStats_,
      ioStats_,
      fileHandleFactory_,
      ioExecutor_,
      scanSpec_,
      columnHandles_);

  return reader;
}

} // namespace facebook::velox::connector::hive::iceberg
