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

#include "velox/dwio/common/ScanSpec.h"

#include "velox/core/Expressions.h"
#include "velox/dwio/common/Statistics.h"

namespace facebook::velox::common {

// static
std::string_view ScanSpec::columnTypeString(ScanSpec::ColumnType columnType) {
  switch (columnType) {
    case ScanSpec::ColumnType::kRegular:
      return "REGULAR";
    case ScanSpec::ColumnType::kRowIndex:
      return "ROW_INDEX";
    case ScanSpec::ColumnType::kComposite:
      return "COMPOSITE";
    default:
      VELOX_UNREACHABLE(
          "Unrecognized ColumnType: {}", static_cast<int8_t>(columnType));
  }
}

ScanSpec* ScanSpec::getOrCreateChild(const std::string& name) {
  if (auto it = this->childByFieldName_.find(name);
      it != this->childByFieldName_.end()) {
    return it->second;
  }
  this->children_.push_back(std::make_unique<ScanSpec>(name));
  auto* child = this->children_.back().get();
  this->childByFieldName_[child->fieldName()] = child;
  return child;
}

ScanSpec* ScanSpec::getOrCreateChild(const Subfield& subfield) {
  auto* container = this;
  const auto& path = subfield.path();
  for (size_t depth = 0; depth < path.size(); ++depth) {
    const auto element = path[depth].get();
    VELOX_CHECK_EQ(element->kind(), kNestedField);
    auto* nestedField = static_cast<const Subfield::NestedField*>(element);
    container = container->getOrCreateChild(nestedField->name());
  }
  return container;
}

bool ScanSpec::compareTimeToDropValue(
    const std::shared_ptr<ScanSpec>& left,
    const std::shared_ptr<ScanSpec>& right) {
  if (left->hasFilter() && right->hasFilter()) {
    if (!disableStatsBasedFilterReorder_ &&
        (left->selectivity_.numIn() || right->selectivity_.numIn())) {
      return left->selectivity_.timeToDropValue() <
          right->selectivity_.timeToDropValue();
    }
    // Integer filters are before other filters if there is no
    // history data.
    if (left->filter_ && right->filter_) {
      if (left->filter_->kind() == right->filter_->kind()) {
        return left->fieldName_ < right->fieldName_;
      }
      return left->filter_->kind() < right->filter_->kind();
    }
    // If hasFilter() is true but 'filter_' is nullptr, we have a filter
    // on complex type members. The simple type filter goes first.
    if (left->filter_) {
      return true;
    }
    if (right->filter_) {
      return false;
    }
    return left->fieldName_ < right->fieldName_;
  }

  if (left->hasFilter()) {
    return true;
  }
  if (right->hasFilter()) {
    return false;
  }
  return left->fieldName_ < right->fieldName_;
}

uint64_t ScanSpec::newRead() {
  // NOTE: in case of split preload, a new split might see zero reads but
  // non-empty filter stats. Hence we need to avoid stats triggered filter
  // reordering even on the first read if 'disableStatsBasedFilterReorder_' is
  // set.
  if (numReads_ == 0 ||
      (!disableStatsBasedFilterReorder_ &&
       !std::is_sorted(
           children_.begin(),
           children_.end(),
           [this](
               const std::shared_ptr<ScanSpec>& left,
               const std::shared_ptr<ScanSpec>& right) {
             return compareTimeToDropValue(left, right);
           }))) {
    reorder();
  }
  return ++numReads_;
}

void ScanSpec::reorder() {
  if (children_.empty()) {
    return;
  }
  // Make sure 'stableChildren_' is initialized.
  stableChildren();
  std::sort(
      children_.begin(),
      children_.end(),
      [this](
          const std::shared_ptr<ScanSpec>& left,
          const std::shared_ptr<ScanSpec>& right) {
        return compareTimeToDropValue(left, right);
      });
}

void ScanSpec::enableFilterInSubTree(bool value) {
  filterDisabled_ = !value;
  for (auto& child : children_) {
    child->enableFilterInSubTree(value);
  }
}

const std::vector<ScanSpec*>& ScanSpec::stableChildren() {
  std::lock_guard<std::mutex> l(mutex_);
  if (stableChildren_.empty()) {
    stableChildren_.reserve(children_.size());
    for (auto& child : children_) {
      stableChildren_.push_back(child.get());
    }
  }
  return stableChildren_;
}

bool ScanSpec::hasFilter() const {
  if (filterDisabled_) {
    return false;
  }
  if (hasFilter_.has_value()) {
    return hasFilter_.value();
  }
  if (!isConstant() && filter()) {
    hasFilter_ = true;
    return true;
  }
  for (auto& child : children_) {
    if (!child->isArrayElementOrMapEntry_ && child->hasFilter()) {
      hasFilter_ = true;
      return true;
    }
  }
  hasFilter_ = false;
  return false;
}

bool ScanSpec::hasFilterApplicableToConstant() const {
  if (filter_) {
    return true;
  }
  for (auto& child : children_) {
    if (!child->isArrayElementOrMapEntry_ &&
        child->hasFilterApplicableToConstant()) {
      return true;
    }
  }
  return false;
}

bool ScanSpec::testNull() const {
  auto* filter = this->filter();
  if (filter && !filter->testNull()) {
    return false;
  }
  for (auto& child : children_) {
    if (!child->isArrayElementOrMapEntry_ && !child->testNull()) {
      return false;
    }
  }
  return true;
}

void ScanSpec::moveAdaptationFrom(ScanSpec& other) {
  VELOX_CHECK(!filterDisabled_);
  // moves the filters and filter order from 'other'.
  for (auto& child : children_) {
    auto it = other.childByFieldName_.find(child->fieldName_);
    if (it == other.childByFieldName_.end()) {
      continue;
    }
    auto* otherChild = it->second;
    if (!child->isConstant() && !otherChild->isConstant()) {
      // If other child is constant, a possible filter on a
      // constant will have been evaluated at split start time. If
      // 'child' is constant there is no adaptation that can be
      // received.
      child->filter_ = std::move(otherChild->filter_);
      child->selectivity_ = otherChild->selectivity_;
    }
  }
}

namespace {
bool testIntFilter(
    const common::Filter* filter,
    dwio::common::IntegerColumnStatistics* intStats,
    bool mayHaveNull) {
  if (!intStats) {
    return true;
  }

  if (intStats->getMinimum().has_value() &&
      intStats->getMaximum().has_value()) {
    return filter->testInt64Range(
        intStats->getMinimum().value(),
        intStats->getMaximum().value(),
        mayHaveNull);
  }

  // only min value
  if (intStats->getMinimum().has_value()) {
    return filter->testInt64Range(
        intStats->getMinimum().value(),
        std::numeric_limits<int64_t>::max(),
        mayHaveNull);
  }

  // only max value
  if (intStats->getMaximum().has_value()) {
    return filter->testInt64Range(
        std::numeric_limits<int64_t>::min(),
        intStats->getMaximum().value(),
        mayHaveNull);
  }

  return true;
}

bool testDoubleFilter(
    const common::Filter* filter,
    dwio::common::DoubleColumnStatistics* doubleStats,
    bool mayHaveNull) {
  if (!doubleStats) {
    return true;
  }

  if (doubleStats->getMinimum().has_value() &&
      doubleStats->getMaximum().has_value()) {
    return filter->testDoubleRange(
        doubleStats->getMinimum().value(),
        doubleStats->getMaximum().value(),
        mayHaveNull);
  }

  // only min value
  if (doubleStats->getMinimum().has_value()) {
    return filter->testDoubleRange(
        doubleStats->getMinimum().value(),
        std::numeric_limits<double>::max(),
        mayHaveNull);
  }

  // only max value
  if (doubleStats->getMaximum().has_value()) {
    return filter->testDoubleRange(
        std::numeric_limits<double>::lowest(),
        doubleStats->getMaximum().value(),
        mayHaveNull);
  }

  return true;
}

bool testStringFilter(
    const common::Filter* filter,
    dwio::common::StringColumnStatistics* stringStats,
    bool mayHaveNull) {
  if (!stringStats) {
    return true;
  }

  if (stringStats->getMinimum().has_value() &&
      stringStats->getMaximum().has_value()) {
    const auto& min = stringStats->getMinimum().value();
    const auto& max = stringStats->getMaximum().value();
    return filter->testBytesRange(min, max, mayHaveNull);
  }

  // only min value
  if (stringStats->getMinimum().has_value()) {
    const auto& min = stringStats->getMinimum().value();
    return filter->testBytesRange(min, std::nullopt, mayHaveNull);
  }

  // only max value
  if (stringStats->getMaximum().has_value()) {
    const auto& max = stringStats->getMaximum().value();
    return filter->testBytesRange(std::nullopt, max, mayHaveNull);
  }

  return true;
}

bool testBoolFilter(
    const common::Filter* filter,
    dwio::common::BooleanColumnStatistics* boolStats) {
  const auto trueCount = boolStats->getTrueCount();
  const auto falseCount = boolStats->getFalseCount();
  if (trueCount.has_value() && falseCount.has_value()) {
    if (trueCount.value() == 0) {
      if (!filter->testBool(false)) {
        return false;
      }
    } else if (falseCount.value() == 0) {
      if (!filter->testBool(true)) {
        return false;
      }
    }
  }
  return true;
}

} // namespace

bool testFilter(
    const common::Filter* filter,
    dwio::common::ColumnStatistics* stats,
    uint64_t totalRows,
    const TypePtr& type) {
  bool mayHaveNull{true};

  // Has-null statistics is often not set. Hence, we supplement it with
  // number-of-values statistic to detect no-null columns more often.
  // Number-of-values is the number of non-null values. When it is equal to
  // total number of values, we know there are no nulls.
  if (stats->getNumberOfValues().has_value()) {
    if (stats->getNumberOfValues().value() == 0) {
      // Column is all null.
      return filter->testNull();
    }
    mayHaveNull = stats->getNumberOfValues().value() < totalRows;
  }

  if (!mayHaveNull && filter->kind() == common::FilterKind::kIsNull) {
    // IS NULL filter cannot pass.
    return false;
  }

  if (mayHaveNull && filter->testNull()) {
    return true;
  }
  if (type->isDecimal()) {
    // The min and max value in the metadata for decimal type in Parquet can be
    // stored in different physical types, including int32, int64 and
    // fixed_len_byte_array. The loading of them is not supported in Metadata.
    return true;
  }
  switch (type->kind()) {
    case TypeKind::BIGINT:
    case TypeKind::INTEGER:
    case TypeKind::SMALLINT:
    case TypeKind::TINYINT: {
      auto* intStats =
          dynamic_cast<dwio::common::IntegerColumnStatistics*>(stats);
      return testIntFilter(filter, intStats, mayHaveNull);
    }
    case TypeKind::REAL:
    case TypeKind::DOUBLE: {
      auto* doubleStats =
          dynamic_cast<dwio::common::DoubleColumnStatistics*>(stats);
      return testDoubleFilter(filter, doubleStats, mayHaveNull);
    }
    case TypeKind::BOOLEAN: {
      auto* boolStats =
          dynamic_cast<dwio::common::BooleanColumnStatistics*>(stats);
      return testBoolFilter(filter, boolStats);
    }
    case TypeKind::VARCHAR: {
      auto* stringStats =
          dynamic_cast<dwio::common::StringColumnStatistics*>(stats);
      return testStringFilter(filter, stringStats, mayHaveNull);
    }
    default:
      break;
  }

  return true;
}

ScanSpec& ScanSpec::getChildByChannel(column_index_t channel) {
  for (auto& child : children_) {
    if (child->channel_ == channel) {
      return *child;
    }
  }
  VELOX_FAIL("No ScanSpec produces channel {}", channel);
}

std::string ScanSpec::toString() const {
  std::stringstream out;
  if (!fieldName_.empty()) {
    out << fieldName_;
    if (filter_) {
      out << " filter " << filter_->toString();
      if (filterDisabled_) {
        out << " disabled";
      }
    }
    if (isConstant()) {
      out << " constant";
    }
    if (deltaUpdate_) {
      out << " deltaUpdate_=" << deltaUpdate_;
    }
    if (!metadataFilters_.empty()) {
      out << " metadata_filters(" << metadataFilters_.size() << ")";
    }
  }
  if (!children_.empty()) {
    out << " (";
    for (auto& child : children_) {
      out << child->toString() << ", ";
    }
    out << ")";
  }
  return out.str();
}

ScanSpec* ScanSpec::addField(const std::string& name, column_index_t channel) {
  auto child = getOrCreateChild(name);
  child->setProjectOut(true);
  child->setChannel(channel);
  return child;
}

ScanSpec* ScanSpec::addFieldRecursively(
    const std::string& name,
    const Type& type,
    column_index_t channel) {
  auto* child = addField(name, channel);
  child->addAllChildFields(type);
  return child;
}

ScanSpec* ScanSpec::addMapKeyField() {
  auto* child = addField(kMapKeysFieldName, kNoChannel);
  child->isArrayElementOrMapEntry_ = true;
  return child;
}

ScanSpec* ScanSpec::addMapKeyFieldRecursively(const Type& type) {
  auto* child = addFieldRecursively(kMapKeysFieldName, type, kNoChannel);
  child->isArrayElementOrMapEntry_ = true;
  return child;
}

ScanSpec* ScanSpec::addMapValueField() {
  auto* child = addField(kMapValuesFieldName, kNoChannel);
  child->isArrayElementOrMapEntry_ = true;
  return child;
}

ScanSpec* ScanSpec::addMapValueFieldRecursively(const Type& type) {
  auto* child = addFieldRecursively(kMapValuesFieldName, type, kNoChannel);
  child->isArrayElementOrMapEntry_ = true;
  return child;
}

ScanSpec* ScanSpec::addArrayElementField() {
  auto* child = addField(kArrayElementsFieldName, kNoChannel);
  child->isArrayElementOrMapEntry_ = true;
  return child;
}

ScanSpec* ScanSpec::addArrayElementFieldRecursively(const Type& type) {
  auto* child = addFieldRecursively(kArrayElementsFieldName, type, kNoChannel);
  child->isArrayElementOrMapEntry_ = true;
  return child;
}

void ScanSpec::addAllChildFields(const Type& type) {
  switch (type.kind()) {
    case TypeKind::ROW: {
      auto& rowType = type.asRow();
      for (auto i = 0; i < type.size(); ++i) {
        addFieldRecursively(rowType.nameOf(i), *type.childAt(i), i);
      }
      break;
    }
    case TypeKind::MAP:
      addMapKeyFieldRecursively(*type.childAt(0));
      addMapValueFieldRecursively(*type.childAt(1));
      break;
    case TypeKind::ARRAY:
      addArrayElementFieldRecursively(*type.childAt(0));
      break;
    default:
      break;
  }
}

namespace {

template <TypeKind kKind>
void filterSimpleVectorRows(
    const BaseVector& vector,
    const Filter& filter,
    vector_size_t size,
    uint64_t* result) {
  VELOX_CHECK(size == 0 || result);
  using T = typename TypeTraits<kKind>::NativeType;
  auto* simpleVector = vector.asChecked<SimpleVector<T>>();
  bits::forEachSetBit(result, 0, size, [&](auto i) {
    if (simpleVector->isNullAt(i)) {
      if (!filter.testNull()) {
        bits::clearBit(result, i);
      }
    } else if (!applyFilter(filter, simpleVector->valueAt(i))) {
      bits::clearBit(result, i);
    }
  });
}

void filterRows(
    const BaseVector& vector,
    const Filter& filter,
    vector_size_t size,
    uint64_t* result) {
  VELOX_CHECK_LE(size, vector.size());
  switch (vector.typeKind()) {
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
      VELOX_CHECK(
          filter.kind() == FilterKind::kIsNull ||
              filter.kind() == FilterKind::kIsNotNull,
          "Complex type can only take null filter, got {}",
          filter.toString());
      bits::forEachSetBit(result, 0, size, [&](auto i) {
        bool pass =
            vector.isNullAt(i) ? filter.testNull() : filter.testNonNull();
        if (!pass) {
          bits::clearBit(result, i);
        }
      });
      break;
    default:
      return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          filterSimpleVectorRows,
          vector.typeKind(),
          vector,
          filter,
          size,
          result);
  }
}

} // namespace

void ScanSpec::applyFilter(
    const BaseVector& vector,
    vector_size_t size,
    uint64_t* result) const {
  if (filter_) {
    filterRows(vector, *filter_, size, result);
  }
  if (!vector.type()->isRow()) {
    // Filter on MAP or ARRAY children are pruning, and won't affect correctness
    // of the result.
    return;
  }
  auto& rowType = vector.type()->asRow();
  auto* rowVector = vector.asChecked<RowVector>();
  for (int i = 0; i < rowType.size(); ++i) {
    if (auto* child = childByName(rowType.nameOf(i))) {
      child->applyFilter(*rowVector->childAt(i), size, result);
    }
  }
}

} // namespace facebook::velox::common
