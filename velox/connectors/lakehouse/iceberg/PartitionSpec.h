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

#include <optional>
#include <string>
#include <vector>
#include "velox/common/serialization/Serializable.h"

namespace facebook::velox::connector::lakehouse::iceberg {

enum class TransformType {
  kIdentity,
  kHour,
  kDay,
  kMonth,
  kYear,
  kBucket,
  kTruncate
};

inline std::string transformTypeToString(TransformType type) {
  switch (type) {
    case TransformType::kIdentity:
      return "identity";
    case TransformType::kHour:
      return "hour";
    case TransformType::kDay:
      return "day";
    case TransformType::kMonth:
      return "month";
    case TransformType::kYear:
      return "year";
    case TransformType::kBucket:
      return "bucket";
    case TransformType::kTruncate:
      return "truncate";
  }
  VELOX_UNREACHABLE("Unknown TransformType");
}

inline TransformType transformTypeFromString(const std::string& str) {
  if (str == "identity") {
    return TransformType::kIdentity;
  } else if (str == "hour") {
    return TransformType::kHour;
  } else if (str == "day") {
    return TransformType::kDay;
  } else if (str == "month") {
    return TransformType::kMonth;
  } else if (str == "year") {
    return TransformType::kYear;
  } else if (str == "bucket") {
    return TransformType::kBucket;
  } else if (str == "truncate") {
    return TransformType::kTruncate;
  } else {
    VELOX_USER_FAIL("Unknown TransformType: {}", str);
  }
}

struct IcebergPartitionSpec : public ISerializable {
  struct Field : public ISerializable {
    // The column name and type of this partition field as it appears in the
    // partition spec. The column can be a nested column in struct field.
    std::string name;

    // The transform type applied to the source field (e.g., kIdentity, kBucket,
    // kTruncate, etc.).
    TransformType transformType;

    // Optional parameter for transforms that require configuration
    // (e.g., bucket count or truncate width).
    std::optional<int32_t> parameter;

    Field(
        const std::string& _name,
        TransformType _transform,
        std::optional<int32_t> _parameter)
        : name(_name), transformType(_transform), parameter(_parameter) {}

    folly::dynamic serialize() const override {
      folly::dynamic obj = folly::dynamic::object;
      obj["name"] = "Field";
      obj["fieldName"] = name;
      obj["transformType"] = transformTypeToString(transformType);
      if (parameter.has_value()) {
        obj["parameter"] = parameter.value();
      } else {
        obj["parameter"] = nullptr;
      }
      return obj;
    }

    static std::shared_ptr<Field> create(const folly::dynamic& obj, void* context) {
      VELOX_CHECK(obj.isObject(), "Field::create expects object");
      
      const auto* fieldNamePtr = obj.get_ptr("fieldName");
      VELOX_CHECK(fieldNamePtr, "Field::create: missing 'fieldName'");
      VELOX_CHECK(fieldNamePtr->isString(), "Field::create: 'fieldName' must be string");
      auto fieldName = fieldNamePtr->asString();

      const auto* transformTypePtr = obj.get_ptr("transformType");
      VELOX_CHECK(transformTypePtr, "Field::create: missing 'transformType'");
      VELOX_CHECK(transformTypePtr->isString(), "Field::create: 'transformType' must be string");
      auto transformType = transformTypeFromString(transformTypePtr->asString());

      std::optional<int32_t> parameter = std::nullopt;
      const auto* parameterPtr = obj.get_ptr("parameter");
      if (parameterPtr && !parameterPtr->isNull()) {
        VELOX_CHECK(parameterPtr->isInt(), "Field::create: 'parameter' must be integer if present");
        parameter = static_cast<int32_t>(parameterPtr->asInt());
      }

      return std::make_shared<Field>(fieldName, transformType, parameter);
    }

    static void registerSerDe() {
      auto& registry = DeserializationWithContextRegistryForSharedPtr();
      registry.Register("Field", Field::create);
    }
  };

  const int32_t specId;
  const std::vector<Field> fields;

  IcebergPartitionSpec(int32_t _specId, const std::vector<Field>& _fields)
      : specId(_specId), fields(_fields) {}

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "IcebergPartitionSpec";
    obj["specId"] = specId;
    
    folly::dynamic fieldsArray = folly::dynamic::array;
    fieldsArray.reserve(fields.size());
    for (const auto& field : fields) {
      fieldsArray.push_back(field.serialize());
    }
    obj["fields"] = std::move(fieldsArray);
    return obj;
  }

  static std::shared_ptr<IcebergPartitionSpec> create(const folly::dynamic& obj, void* context) {
    VELOX_CHECK(obj.isObject(), "IcebergPartitionSpec::create expects object");
    
    const auto* specIdPtr = obj.get_ptr("specId");
    VELOX_CHECK(specIdPtr, "IcebergPartitionSpec::create: missing 'specId'");
    VELOX_CHECK(specIdPtr->isInt(), "IcebergPartitionSpec::create: 'specId' must be integer");
    auto specId = static_cast<int32_t>(specIdPtr->asInt());

    const auto* fieldsPtr = obj.get_ptr("fields");
    VELOX_CHECK(fieldsPtr, "IcebergPartitionSpec::create: missing 'fields'");
    VELOX_CHECK(fieldsPtr->isArray(), "IcebergPartitionSpec::create: 'fields' must be array");

    std::vector<Field> deserializedFields;
    deserializedFields.reserve(fieldsPtr->size());
    for (const auto& fieldObj : *fieldsPtr) {
      auto field = Field::create(fieldObj, context);
      deserializedFields.push_back(*field);
    }

    return std::make_shared<IcebergPartitionSpec>(specId, std::move(deserializedFields));
  }

  static void registerSerDe() {
    Field::registerSerDe();
    auto& registry = DeserializationWithContextRegistryForSharedPtr();
    registry.Register("IcebergPartitionSpec", IcebergPartitionSpec::create);
  }
};

} // namespace facebook::velox::connector::lakehouse::iceberg
