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

#include <gtest/gtest.h>

#include "velox/connectors/lakehouse/iceberg/PartitionSpec.h"
#include "velox/connectors/lakehouse/iceberg/tests/IcebergTestBase.h"

namespace facebook::velox::connector::lakehouse::iceberg::test {

class PartitionSpecTest : public ::testing::Test {
 public:
  void SetUp() override {
    // Initialize memory pools.
    if (!memory::MemoryManager::testInstance()) {
      memory::initializeMemoryManager(memory::MemoryManager::Options{});
    }
    rootPool_ = memory::memoryManager()->addRootPool("PartitionSpecTest");
    pool_ = rootPool_->addLeafChild("leaf");

    // Register SerDe for PartitionSpec
    IcebergPartitionSpec::registerSerDe();
  }

  void TearDown() override {
    pool_.reset();
    rootPool_.reset();
  }

  memory::MemoryPool* pool() {
    return pool_.get();
  }

 private:
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(PartitionSpecTest, transformTypeConversion) {
  // Test enum to string conversion
  EXPECT_EQ("identity", transformTypeToString(TransformType::kIdentity));
  EXPECT_EQ("hour", transformTypeToString(TransformType::kHour));
  EXPECT_EQ("day", transformTypeToString(TransformType::kDay));
  EXPECT_EQ("month", transformTypeToString(TransformType::kMonth));
  EXPECT_EQ("year", transformTypeToString(TransformType::kYear));
  EXPECT_EQ("bucket", transformTypeToString(TransformType::kBucket));
  EXPECT_EQ("truncate", transformTypeToString(TransformType::kTruncate));

  // Test string to enum conversion
  EXPECT_EQ(TransformType::kIdentity, transformTypeFromString("identity"));
  EXPECT_EQ(TransformType::kHour, transformTypeFromString("hour"));
  EXPECT_EQ(TransformType::kDay, transformTypeFromString("day"));
  EXPECT_EQ(TransformType::kMonth, transformTypeFromString("month"));
  EXPECT_EQ(TransformType::kYear, transformTypeFromString("year"));
  EXPECT_EQ(TransformType::kBucket, transformTypeFromString("bucket"));
  EXPECT_EQ(TransformType::kTruncate, transformTypeFromString("truncate"));

  // Test invalid string
  EXPECT_THROW(transformTypeFromString("invalid"), VeloxUserError);
}

TEST_F(PartitionSpecTest, fieldSerializeRoundTrip) {
  // Test Field with parameter
  IcebergPartitionSpec::Field fieldWithParam("bucket_column", TransformType::kBucket, 10);
  auto serialized = fieldWithParam.serialize();
  
  EXPECT_EQ(serialized["name"].asString(), "Field");
  EXPECT_EQ(serialized["fieldName"].asString(), "bucket_column");
  EXPECT_EQ(serialized["transformType"].asString(), "bucket");
  EXPECT_EQ(serialized["parameter"].asInt(), 10);

  auto deserialized = IcebergPartitionSpec::Field::create(serialized, pool());
  EXPECT_EQ(deserialized->name, "bucket_column");
  EXPECT_EQ(deserialized->transformType, TransformType::kBucket);
  EXPECT_TRUE(deserialized->parameter.has_value());
  EXPECT_EQ(deserialized->parameter.value(), 10);

  // Test Field without parameter
  IcebergPartitionSpec::Field fieldWithoutParam("identity_column", TransformType::kIdentity, std::nullopt);
  auto serialized2 = fieldWithoutParam.serialize();
  
  EXPECT_EQ(serialized2["name"].asString(), "Field");
  EXPECT_EQ(serialized2["fieldName"].asString(), "identity_column");
  EXPECT_EQ(serialized2["transformType"].asString(), "identity");
  EXPECT_TRUE(serialized2["parameter"].isNull());

  auto deserialized2 = IcebergPartitionSpec::Field::create(serialized2, pool());
  EXPECT_EQ(deserialized2->name, "identity_column");
  EXPECT_EQ(deserialized2->transformType, TransformType::kIdentity);
  EXPECT_FALSE(deserialized2->parameter.has_value());
}

TEST_F(PartitionSpecTest, partitionSpecSerializeRoundTrip) {
  // Create a partition spec with multiple fields
  std::vector<IcebergPartitionSpec::Field> fields;
  fields.emplace_back("date_column", TransformType::kDay, std::nullopt);
  fields.emplace_back("bucket_column", TransformType::kBucket, 16);
  fields.emplace_back("truncate_column", TransformType::kTruncate, 10);
  fields.emplace_back("identity_column", TransformType::kIdentity, std::nullopt);

  IcebergPartitionSpec spec(42, fields);

  // Serialize
  auto serialized = spec.serialize();
  EXPECT_EQ(serialized["name"].asString(), "IcebergPartitionSpec");
  EXPECT_EQ(serialized["specId"].asInt(), 42);
  EXPECT_TRUE(serialized["fields"].isArray());
  EXPECT_EQ(serialized["fields"].size(), 4);

  // Verify individual fields in serialized output
  auto& serializedFields = serialized["fields"];
  EXPECT_EQ(serializedFields[0]["fieldName"].asString(), "date_column");
  EXPECT_EQ(serializedFields[0]["transformType"].asString(), "day");
  EXPECT_TRUE(serializedFields[0]["parameter"].isNull());

  EXPECT_EQ(serializedFields[1]["fieldName"].asString(), "bucket_column");
  EXPECT_EQ(serializedFields[1]["transformType"].asString(), "bucket");
  EXPECT_EQ(serializedFields[1]["parameter"].asInt(), 16);

  EXPECT_EQ(serializedFields[2]["fieldName"].asString(), "truncate_column");
  EXPECT_EQ(serializedFields[2]["transformType"].asString(), "truncate");
  EXPECT_EQ(serializedFields[2]["parameter"].asInt(), 10);

  EXPECT_EQ(serializedFields[3]["fieldName"].asString(), "identity_column");
  EXPECT_EQ(serializedFields[3]["transformType"].asString(), "identity");
  EXPECT_TRUE(serializedFields[3]["parameter"].isNull());

  // Deserialize
  auto deserialized = IcebergPartitionSpec::create(serialized, pool());
  EXPECT_EQ(deserialized->specId, 42);
  EXPECT_EQ(deserialized->fields.size(), 4);

  // Verify deserialized fields
  const auto& deserializedFields = deserialized->fields;
  
  EXPECT_EQ(deserializedFields[0].name, "date_column");
  EXPECT_EQ(deserializedFields[0].transformType, TransformType::kDay);
  EXPECT_FALSE(deserializedFields[0].parameter.has_value());

  EXPECT_EQ(deserializedFields[1].name, "bucket_column");
  EXPECT_EQ(deserializedFields[1].transformType, TransformType::kBucket);
  EXPECT_TRUE(deserializedFields[1].parameter.has_value());
  EXPECT_EQ(deserializedFields[1].parameter.value(), 16);

  EXPECT_EQ(deserializedFields[2].name, "truncate_column");
  EXPECT_EQ(deserializedFields[2].transformType, TransformType::kTruncate);
  EXPECT_TRUE(deserializedFields[2].parameter.has_value());
  EXPECT_EQ(deserializedFields[2].parameter.value(), 10);

  EXPECT_EQ(deserializedFields[3].name, "identity_column");
  EXPECT_EQ(deserializedFields[3].transformType, TransformType::kIdentity);
  EXPECT_FALSE(deserializedFields[3].parameter.has_value());
}

TEST_F(PartitionSpecTest, emptyPartitionSpec) {
  // Test with empty fields
  std::vector<IcebergPartitionSpec::Field> emptyFields;
  IcebergPartitionSpec emptySpec(0, emptyFields);

  auto serialized = emptySpec.serialize();
  EXPECT_EQ(serialized["name"].asString(), "IcebergPartitionSpec");
  EXPECT_EQ(serialized["specId"].asInt(), 0);
  EXPECT_TRUE(serialized["fields"].isArray());
  EXPECT_EQ(serialized["fields"].size(), 0);

  auto deserialized = IcebergPartitionSpec::create(serialized, pool());
  EXPECT_EQ(deserialized->specId, 0);
  EXPECT_EQ(deserialized->fields.size(), 0);
}

TEST_F(PartitionSpecTest, serializationErrors) {
  // Test invalid Field creation
  folly::dynamic invalidField = folly::dynamic::object;
  invalidField["name"] = "Field";
  // Missing fieldName should cause error
  EXPECT_THROW(IcebergPartitionSpec::Field::create(invalidField, pool()), VeloxException);

  // Test invalid PartitionSpec creation
  folly::dynamic invalidSpec = folly::dynamic::object;
  invalidSpec["name"] = "IcebergPartitionSpec";
  // Missing specId should cause error
  EXPECT_THROW(IcebergPartitionSpec::create(invalidSpec, pool()), VeloxException);
}

} // namespace facebook::velox::connector::lakehouse::iceberg::test