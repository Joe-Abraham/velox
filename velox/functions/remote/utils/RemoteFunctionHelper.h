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

#include <string>
#include <vector>
#include "velox/expression/Expr.h"
#include "velox/type/Type.h"
#include "velox/type/fbhive/HiveTypeParser.h"

namespace facebook::velox::functions {
std::string getFunctionName(
    const std::string& prefix,
    const std::string& functionName);

TypePtr deserializeType(const std::string& input);

RowTypePtr deserializeArgTypes(const std::vector<std::string>& argTypes);

std::vector<core::TypedExprPtr> getExpressions(
    const RowTypePtr& inputType,
    const TypePtr& returnType,
    const std::string& functionName);

} // namespace facebook::velox::functions
