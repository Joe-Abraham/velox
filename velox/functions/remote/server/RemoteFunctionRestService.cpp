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

#include "velox/functions/remote/server/RemoteFunctionRestService.h"
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <serializers/PrestoSerializer.h>

#include "velox/expression/Expr.h"
#include "velox/type/fbhive/HiveTypeParser.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::functions {

namespace {
struct InternalFunctionSignature {
  std::vector<std::string> argumentTypes;
  std::string returnType;
};

// Initialize a map with function details
std::map<std::string, InternalFunctionSignature> internalFunctionSignatureMap =
    {
        {"remote_abs", {{"integer"}, "integer"}},
        {"remote_plus", {{"bigint", "bigint"}, "bigint"}},
        {"remote_divide", {{"double", "double"}, "double"}},
        {"remote_substr", {{"varchar", "integer"}, "varchar"}},
        // Add more functions here as needed
};

TypePtr deserializeType(const std::string& input) {
  // Use hive type parser/serializer.
  return type::fbhive::HiveTypeParser().parse(input);
}

RowTypePtr deserializeArgTypes(const std::vector<std::string>& argTypes) {
  const size_t argCount = argTypes.size();

  std::vector<TypePtr> argumentTypes;
  std::vector<std::string> typeNames;
  argumentTypes.reserve(argCount);
  typeNames.reserve(argCount);

  for (size_t i = 0; i < argCount; ++i) {
    argumentTypes.emplace_back(deserializeType(argTypes[i]));
    typeNames.emplace_back(fmt::format("c{}", i));
  }
  return ROW(std::move(typeNames), std::move(argumentTypes));
}

std::string getFunctionName(
    const std::string& prefix,
    const std::string& functionName) {
  return prefix.empty() ? functionName
                        : fmt::format("{}.{}", prefix, functionName);
}
} // namespace

std::vector<core::TypedExprPtr> getExpressions(
    const RowTypePtr& inputType,
    const TypePtr& returnType,
    const std::string& functionName) {
  std::vector<core::TypedExprPtr> inputs;
  for (size_t i = 0; i < inputType->size(); ++i) {
    inputs.push_back(std::make_shared<core::FieldAccessTypedExpr>(
        inputType->childAt(i), inputType->nameOf(i)));
  }

  return {std::make_shared<core::CallTypedExpr>(
      returnType, std::move(inputs), functionName)};
}

void RestRequestHandler::onRequest(
    std::unique_ptr<HTTPMessage> headers) noexcept {
  const std::string& path = headers->getURL();

  // Split the path by '/'
  std::vector<std::string> pathComponents;
  folly::split('/', path, pathComponents);

  // Check if the path has enough components
  if (pathComponents.size() >= 5) {
    // Extract the functionName from the path
    // Assuming the functionName is the 5th component
    functionName_ = pathComponents[6];
  }
}

void RestRequestHandler::onEOM() noexcept {
  try {
    const auto& functionSignature =
        internalFunctionSignatureMap.at(functionName_);

    auto inputType = deserializeArgTypes(functionSignature.argumentTypes);
    auto returnType = deserializeType(functionSignature.returnType);

    serializer::presto::PrestoVectorSerde serde;
    auto inputVector = IOBufToRowVector(*body_, inputType, *pool_, &serde);

    const vector_size_t numRows = inputVector->size();
    SelectivityVector rows{numRows};

    // Expression boilerplate.
    auto queryCtx = core::QueryCtx::create();
    core::ExecCtx execCtx{pool_.get(), queryCtx.get()};
    exec::ExprSet exprSet{
        getExpressions(
            inputType,
            returnType,
            getFunctionName(functionPrefix_, functionName_)),
        &execCtx};
    exec::EvalCtx evalCtx(&execCtx, &exprSet, inputVector.get());

    std::vector<VectorPtr> expressionResult;
    exprSet.eval(rows, evalCtx, expressionResult);

    // Create output vector.
    auto outputRowVector = std::make_shared<RowVector>(
        pool_.get(), ROW({returnType}), BufferPtr(), numRows, expressionResult);

    auto payload =
        rowVectorToIOBuf(outputRowVector, rows.end(), *pool_, &serde);

    ResponseBuilder(downstream_)
        .status(200, "OK")
        .body(std::make_unique<folly::IOBuf>(payload))
        .sendWithEOM();

  } catch (const std::exception& ex) {
    LOG(ERROR) << ex.what();
    ResponseBuilder(downstream_)
        .status(500, "Internal Server Error")
        .body(folly::IOBuf::copyBuffer(ex.what()))
        .sendWithEOM();
  }
}

void RestRequestHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {
  if (body) {
    body_ = std::move(body);
  }
}

void RestRequestHandler::onUpgrade(UpgradeProtocol /*protocol*/) noexcept {
  // handler doesn't support upgrades
}

void RestRequestHandler::requestComplete() noexcept {
  delete this;
}

void RestRequestHandler::onError(ProxygenError /*err*/) noexcept {
  delete this;
}

// ErrorHandler
ErrorHandler::ErrorHandler(int statusCode, std::string message)
    : statusCode_(statusCode), message_(std::move(message)) {}

void ErrorHandler::onRequest(std::unique_ptr<HTTPMessage>) noexcept {
  ResponseBuilder(downstream_)
      .status(statusCode_, "Error")
      .body(std::move(message_))
      .sendWithEOM();
}

void ErrorHandler::onEOM() noexcept {}

void ErrorHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {}

void ErrorHandler::onUpgrade(UpgradeProtocol protocol) noexcept {
  // handler doesn't support upgrades
}

void ErrorHandler::requestComplete() noexcept {
  delete this;
}

void ErrorHandler::onError(ProxygenError err) noexcept {
  delete this;
}

// RestRequestHandlerFactory
void RestRequestHandlerFactory::onServerStart(folly::EventBase* evb) noexcept {}

void RestRequestHandlerFactory::onServerStop() noexcept {}

RequestHandler* RestRequestHandlerFactory::onRequest(
    proxygen::RequestHandler*,
    proxygen::HTTPMessage* msg) noexcept {
  if (msg->getMethod() != HTTPMethod::POST) {
    return new ErrorHandler(405, "Only POST method is allowed");
  }
  return new RestRequestHandler(functionPrefix_);
}
} // namespace facebook::velox::functions
