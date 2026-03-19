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

#include "velox/connectors/hive/iceberg/IcebergConnector.h"
#include <gtest/gtest.h>
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/iceberg/IcebergConfig.h"
#include "velox/connectors/hive/iceberg/tests/IcebergTestBase.h"

namespace facebook::velox::connector::hive::iceberg {

namespace {

class IcebergConnectorTest : public test::IcebergTestBase {
 protected:
  static void resetIcebergConnector(
      const std::shared_ptr<const config::ConfigBase>& config) {
    unregisterConnector(test::kIcebergConnectorId);

    IcebergConnectorFactory factory;
    auto icebergConnector =
        factory.newConnector(test::kIcebergConnectorId, config);
    registerConnector(icebergConnector);
  }
};

TEST_F(IcebergConnectorTest, connectorConfiguration) {
  auto customConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{
          {hive::HiveConfig::kEnableFileHandleCache, "true"},
          {hive::HiveConfig::kNumCacheFileHandles, "1000"}});

  resetIcebergConnector(customConfig);

  // Verify connector was registered successfully with custom config.
  auto icebergConnector = getConnector(test::kIcebergConnectorId);
  ASSERT_NE(icebergConnector, nullptr);

  auto config = icebergConnector->connectorConfig();
  ASSERT_NE(config, nullptr);

  hive::HiveConfig hiveConfig(config);
  ASSERT_TRUE(hiveConfig.isFileHandleCacheEnabled());
  ASSERT_EQ(hiveConfig.numCacheFileHandles(), 1000);
}

TEST_F(IcebergConnectorTest, connectorProperties) {
  auto icebergConnector = getConnector(test::kIcebergConnectorId);
  ASSERT_NE(icebergConnector, nullptr);

  ASSERT_TRUE(icebergConnector->canAddDynamicFilter());
  ASSERT_TRUE(icebergConnector->supportsSplitPreload());
  ASSERT_NE(icebergConnector->ioExecutor(), nullptr);
}

TEST_F(IcebergConnectorTest, icebergPropertyTranslation) {
  // Configure using Iceberg-specific property names (as used by Presto
  // coordinator). These should be translated to HiveConfig equivalents.
  auto icebergConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{
          {IcebergConfig::kIcebergMaxPartitionsPerWriter, "2500"},
          {IcebergConfig::kIcebergTargetMaxFileSize, "512MB"}});

  resetIcebergConnector(icebergConfig);

  auto icebergConnector = getConnector(test::kIcebergConnectorId);
  ASSERT_NE(icebergConnector, nullptr);

  auto config = icebergConnector->connectorConfig();
  ASSERT_NE(config, nullptr);

  // The Iceberg property names should have been translated to HiveConfig
  // equivalents.
  hive::HiveConfig hiveConfig(config);
  auto emptySession = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>());
  ASSERT_EQ(hiveConfig.maxPartitionsPerWriters(emptySession.get()), 2500);
  ASSERT_EQ(
      hiveConfig.maxTargetFileSizeBytes(emptySession.get()),
      512UL * 1024 * 1024);
}

TEST_F(IcebergConnectorTest, hivePropertyNamesPreferred) {
  // When both Iceberg and HiveConfig property names are set, the HiveConfig
  // names take precedence (they are not overwritten by translation).
  auto config = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{
          {IcebergConfig::kIcebergMaxPartitionsPerWriter, "2500"},
          {hive::HiveConfig::kMaxPartitionsPerWriters, "100"},
          {IcebergConfig::kIcebergTargetMaxFileSize, "512MB"},
          {hive::HiveConfig::kMaxTargetFileSize, "1GB"}});

  resetIcebergConnector(config);

  auto icebergConnector = getConnector(test::kIcebergConnectorId);
  auto connConfig = icebergConnector->connectorConfig();

  hive::HiveConfig hiveConfig(connConfig);
  auto emptySession = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>());
  // HiveConfig property names should take precedence.
  ASSERT_EQ(hiveConfig.maxPartitionsPerWriters(emptySession.get()), 100);
  ASSERT_EQ(
      hiveConfig.maxTargetFileSizeBytes(emptySession.get()),
      1024UL * 1024 * 1024);
}

TEST_F(IcebergConnectorTest, customConnectorName) {
  // Verify that IcebergConnectorFactory supports a custom connector name.
  IcebergConnectorFactory customFactory("custom-iceberg");
  ASSERT_EQ(customFactory.connectorName(), "custom-iceberg");

  auto connector = customFactory.newConnector(
      "custom-catalog-id",
      std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>()));
  ASSERT_NE(connector, nullptr);
  ASSERT_EQ(connector->connectorId(), "custom-catalog-id");
}

} // namespace

} // namespace facebook::velox::connector::hive::iceberg
