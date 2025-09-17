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

#include <folly/dynamic.h>

#include "velox/connectors/Connector.h"
#include "velox/core/ITypedExpr.h"
#include "velox/type/Filter.h"
#include "velox/type/Subfield.h"
#include "velox/type/Type.h"

namespace facebook::velox::connector::lakehouse::common {

class ColumnHandleBase : public connector::ColumnHandle {
 public:
  enum class ColumnType {
    kPartitionKey,
    kRegular,
    kSynthesized,
  };

  /// NOTE: 'dataType' is the column type in target write table. 'hiveType' is
  /// converted type of the corresponding column in source table which might not
  /// be the same type, and the table scan needs to do data coercion if needs.
  /// The table writer also needs to respect the type difference when processing
  /// input data such as bucket id calculation.
  ColumnHandleBase(
      const std::string& columnName,
      ColumnType columnType,
      TypePtr dataType,
      std::vector<velox::common::Subfield> requiredSubfields = {})
      : columnName_(columnName),
        columnType_(columnType),
        dataType_(std::move(dataType)),
        requiredSubfields_(std::move(requiredSubfields)) {}

  const std::string& name() const override {
    return columnName_;
  }

  ColumnType columnType() const {
    return columnType_;
  }

  const TypePtr& dataType() const {
    return dataType_;
  }

  /// Applies to columns of complex types: arrays, maps and structs.  When a
  /// query uses only some of the subfields, the engine provides the complete
  /// list of required subfields and the connector is free to prune the rest.
  ///
  /// Examples:
  ///  - SELECT a[1], b['x'], x.y FROM t
  ///  - SELECT a FROM t WHERE b['y'] > 10
  ///
  /// Pruning a struct means populating some of the members with null values.
  ///
  /// Pruning a map means dropping keys not listed in the required subfields.
  ///
  /// Pruning arrays means dropping values with indices larger than maximum
  /// required index.
  const std::vector<velox::common::Subfield>& requiredSubfields() const {
    return requiredSubfields_;
  }

  bool isPartitionKey() const {
    return columnType_ == ColumnType::kPartitionKey;
  }

  bool isSynthesized() const {
    return columnType_ == ColumnType::kSynthesized;
  }

  virtual std::string toString() const {
    return toStringBase("ColumnHandleBase");
  }

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("toString() is not implemented in ColumnHandleBase");
  }

  static ColumnHandleBase::ColumnType columnTypeFromName(
      const std::string& name);

 protected:
  std::string toStringBase(const std::string& classNanme) const;
  folly::dynamic serializeBase(std::string_view classNanme) const;

  const std::string columnName_;
  const ColumnType columnType_;
  const TypePtr dataType_;
  const std::vector<velox::common::Subfield> requiredSubfields_;

 private:
  static std::string columnTypeName(ColumnHandleBase::ColumnType columnType);
};

using ColumnHandleBasePtr = std::shared_ptr<const ColumnHandleBase>;

using ColumnHandleBaseMap =
    std::unordered_map<std::string, ColumnHandleBasePtr>;

class TableHandleBase : public ConnectorTableHandle {
 public:
  TableHandleBase(
      std::string connectorId,
      const std::string& tableName,
      bool filterPushdownEnabled,
      velox::common::SubfieldFilters subfieldFilters,
      const core::TypedExprPtr& remainingFilter,
      const RowTypePtr& dataColumns = nullptr,
      const std::unordered_map<std::string, std::string>& tableParameters = {})
      : ConnectorTableHandle(std::move(connectorId)),
        tableName_(tableName),
        filterPushdownEnabled_(filterPushdownEnabled),
        subfieldFilters_(std::move(subfieldFilters)),
        remainingFilter_(remainingFilter),
        dataColumns_(dataColumns),
        tableParameters_(tableParameters) {}

  const std::string& name() const override {
    return tableName_;
  }

  bool isFilterPushdownEnabled() const {
    return filterPushdownEnabled_;
  }

  const velox::common::SubfieldFilters& subfieldFilters() const {
    return subfieldFilters_;
  }

  const core::TypedExprPtr& remainingFilter() const {
    return remainingFilter_;
  }

  // Schema of the table.  Need this for reading TEXTFILE.
  const RowTypePtr& dataColumns() const {
    return dataColumns_;
  }

  const std::unordered_map<std::string, std::string>& tableParameters() const {
    return tableParameters_;
  }

  virtual std::string toString() const override {
    return toStringBase("TableHandleBase");
  }

  virtual folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("serialize() is not implemented in TableHandleBase");
  }

  static ConnectorTableHandlePtr create(
      const folly::dynamic& obj,
      void* context);

 protected:
  folly::dynamic serializeBase(const std::string& className) const;
  std::string toStringBase(const std::string& className) const;

  const std::string tableName_;
  const bool filterPushdownEnabled_;
  const velox::common::SubfieldFilters subfieldFilters_;
  const core::TypedExprPtr remainingFilter_;
  const RowTypePtr dataColumns_;
  const std::unordered_map<std::string, std::string> tableParameters_;
};

using TableHandleBasePtr = std::shared_ptr<const TableHandleBase>;

} // namespace facebook::velox::connector::lakehouse::common
