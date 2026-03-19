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

#include "velox/connectors/hive/iceberg/IcebergConfig.h"

#include "velox/common/config/Config.h"
#include "velox/connectors/hive/HiveConfig.h"

namespace facebook::velox::connector::hive::iceberg {

IcebergConfig::IcebergConfig(
    const std::shared_ptr<const config::ConfigBase>& config)
    : config_(config) {
  VELOX_CHECK_NOT_NULL(
      config_, "Config is null for IcebergConfig initialization");
}

std::string IcebergConfig::functionPrefix() const {
  return config_->get<std::string>(
      kFunctionPrefixConfig, kDefaultFunctionPrefix);
}

// static
std::shared_ptr<const config::ConfigBase> IcebergConfig::translateConfig(
    const std::shared_ptr<const config::ConfigBase>& config) {
  if (!config) {
    return config;
  }

  // Map of Iceberg-specific property names to HiveConfig equivalents.
  static const std::vector<std::pair<std::string, std::string>>
      kPropertyTranslations = {
          {kIcebergMaxPartitionsPerWriter, HiveConfig::kMaxPartitionsPerWriters},
          {kIcebergTargetMaxFileSize, HiveConfig::kMaxTargetFileSize},
      };

  bool needsTranslation = false;
  for (const auto& [icebergKey, hiveKey] : kPropertyTranslations) {
    if (config->valueExists(icebergKey) && !config->valueExists(hiveKey)) {
      needsTranslation = true;
      break;
    }
  }

  if (!needsTranslation) {
    return config;
  }

  auto configMap = config->rawConfigsCopy();
  for (const auto& [icebergKey, hiveKey] : kPropertyTranslations) {
    auto it = configMap.find(icebergKey);
    if (it != configMap.end() && configMap.find(hiveKey) == configMap.end()) {
      configMap[hiveKey] = it->second;
    }
  }
  return std::make_shared<config::ConfigBase>(std::move(configMap));
}

} // namespace facebook::velox::connector::hive::iceberg
