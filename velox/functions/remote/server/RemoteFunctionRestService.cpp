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

#include "RemoteFunctionRestService.h"

#include <boost/beast/version.hpp>
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

std::map<std::string, InternalFunctionSignature> internalFunctionSignatureMap =
    {
        {"remote_abs", {{"integer"}, "integer"}},
        {"remote_plus", {{"bigint", "bigint"}, "bigint"}},
        {"remote_divide", {{"double", "double"}, "double"}},
        {"remote_substr", {{"varchar", "integer"}, "varchar"}},
        // Add more functions here as needed, registerRemoteFunction should be
        // called to use the functions mentioned in this map
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

std::string getFunctionName(
    const std::string& prefix,
    const std::string& functionName) {
  return prefix.empty() ? functionName
                        : fmt::format("{}.{}", prefix, functionName);
}

} // namespace

session::session(
    boost::asio::ip::tcp::socket socket,
    std::string functionPrefix)
    : socket_(std::move(socket)), functionPrefix_(std::move(functionPrefix)) {}

void session::run() {
  do_read();
}

void session::do_read() {
  auto self = shared_from_this();
  boost::beast::http::async_read(
      socket_,
      buffer_,
      req_,
      [self](boost::beast::error_code ec, std::size_t bytes_transferred) {
        self->on_read(ec, bytes_transferred);
      });
}

void session::on_read(
    boost::beast::error_code ec,
    std::size_t bytes_transferred) {
  boost::ignore_unused(bytes_transferred);

  if (ec == boost::beast::http::error::end_of_stream) {
    return do_close();
  }

  if (ec) {
    LOG(ERROR) << "Read error: " << ec.message();
    return;
  }

  handle_request(std::move(req_));
}

void session::handle_request(
    boost::beast::http::request<boost::beast::http::string_body> req) {
  res_.version(req.version());
  res_.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);

  if (req.method() != boost::beast::http::verb::post) {
    res_.result(boost::beast::http::status::method_not_allowed);
    res_.set(boost::beast::http::field::content_type, "text/plain");
    res_.body() = "Only POST method is allowed";
    res_.prepare_payload();

    auto self = shared_from_this();
    boost::beast::http::async_write(
        socket_,
        res_,
        [self](boost::beast::error_code ec, std::size_t bytes_transferred) {
          self->on_write(true, ec, bytes_transferred);
        });
    return;
  }

  std::string path = req.target();

  // Expected path format:
  // /v1/functions/{schema}/{functionName}/{functionId}/{version} Split the
  // path by '/'
  std::vector<std::string> pathComponents;
  folly::split('/', path, pathComponents);

  std::string functionName;
  if (pathComponents.size() >= 7 && pathComponents[1] == "v1" &&
      pathComponents[2] == "functions") {
    functionName = pathComponents[4];
  } else {
    res_.result(boost::beast::http::status::bad_request);
    res_.set(boost::beast::http::field::content_type, "text/plain");
    res_.body() = "Invalid request path";
    res_.prepare_payload();

    auto self = shared_from_this();
    boost::beast::http::async_write(
        socket_,
        res_,
        [self](boost::beast::error_code ec, std::size_t bytes_transferred) {
          self->on_write(true, ec, bytes_transferred);
        });
    return;
  }

  try {
    const auto& functionSignature =
        internalFunctionSignatureMap.at(functionName);

    auto inputType = deserializeArgTypes(functionSignature.argumentTypes);
    auto returnType = deserializeType(functionSignature.returnType);

    serializer::presto::PrestoVectorSerde serde;
    auto inputBuffer = folly::IOBuf::copyBuffer(req.body());
    auto inputVector =
        IOBufToRowVector(*inputBuffer, inputType, *pool_, &serde);

    const vector_size_t numRows = inputVector->size();
    SelectivityVector rows{numRows};

    // Expression boilerplate.
    auto queryCtx = core::QueryCtx::create();
    core::ExecCtx execCtx{pool_.get(), queryCtx.get()};
    exec::ExprSet exprSet{
        getExpressions(
            inputType,
            returnType,
            getFunctionName(functionPrefix_, functionName)),
        &execCtx};
    exec::EvalCtx evalCtx(&execCtx, &exprSet, inputVector.get());

    std::vector<VectorPtr> expressionResult;
    exprSet.eval(rows, evalCtx, expressionResult);

    // Create output vector.
    auto outputRowVector = std::make_shared<RowVector>(
        pool_.get(), ROW({returnType}), BufferPtr(), numRows, expressionResult);

    auto payload =
        rowVectorToIOBuf(outputRowVector, rows.end(), *pool_, &serde);

    res_.result(boost::beast::http::status::ok);
    res_.set(
        boost::beast::http::field::content_type, "application/octet-stream");
    res_.body() = payload.moveToFbString().toStdString();
    res_.prepare_payload();

    auto self = shared_from_this();
    boost::beast::http::async_write(
        socket_,
        res_,
        [self](boost::beast::error_code ec, std::size_t bytes_transferred) {
          self->on_write(false, ec, bytes_transferred);
        });

  } catch (const std::exception& ex) {
    LOG(ERROR) << ex.what();
    res_.result(boost::beast::http::status::internal_server_error);
    res_.set(boost::beast::http::field::content_type, "text/plain");
    res_.body() = ex.what();
    res_.prepare_payload();

    auto self = shared_from_this();
    boost::beast::http::async_write(
        socket_,
        res_,
        [self](boost::beast::error_code ec, std::size_t bytes_transferred) {
          self->on_write(true, ec, bytes_transferred);
        });
  }
}

void session::on_write(
    bool close,
    boost::beast::error_code ec,
    std::size_t bytes_transferred) {
  boost::ignore_unused(bytes_transferred);

  if (ec) {
    LOG(ERROR) << "Write error: " << ec.message();
    return;
  }

  if (close) {
    return do_close();
  }

  req_ = {};

  do_read();
}

void session::do_close() {
  boost::beast::error_code ec;
  socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_send, ec);
}

listener::listener(
    boost::asio::io_context& ioc,
    boost::asio::ip::tcp::endpoint endpoint,
    std::string functionPrefix)
    : ioc_(ioc), acceptor_(ioc), functionPrefix_(std::move(functionPrefix)) {
  boost::beast::error_code ec;

  acceptor_.open(endpoint.protocol(), ec);
  if (ec) {
    LOG(ERROR) << "Open error: " << ec.message();
    return;
  }

  acceptor_.set_option(boost::asio::socket_base::reuse_address(true), ec);
  if (ec) {
    LOG(ERROR) << "Set_option error: " << ec.message();
    return;
  }

  acceptor_.bind(endpoint, ec);
  if (ec) {
    LOG(ERROR) << "Bind error: " << ec.message();
    return;
  }

  acceptor_.listen(boost::asio::socket_base::max_listen_connections, ec);
  if (ec) {
    LOG(ERROR) << "Listen error: " << ec.message();
    return;
  }
}

void listener::run() {
  do_accept();
}

void listener::do_accept() {
  acceptor_.async_accept(
      [self = shared_from_this()](
          boost::beast::error_code ec, boost::asio::ip::tcp::socket socket) {
        self->on_accept(ec, std::move(socket));
      });
}

void listener::on_accept(
    boost::beast::error_code ec,
    boost::asio::ip::tcp::socket socket) {
  if (ec) {
    LOG(ERROR) << "Accept error: " << ec.message();
  } else {
    std::make_shared<session>(std::move(socket), functionPrefix_)->run();
  }
  do_accept();
}

} // namespace facebook::velox::functions
