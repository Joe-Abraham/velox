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

#include "velox/functions/remote/utils/restserver/RemoteFunctionRestService.h"

#include <boost/beast/version.hpp>
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/fbhive/HiveTypeParser.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::functions {

RestSession::RestSession(
    boost::asio::ip::tcp::socket socket,
    std::string functionPrefix)
    : socket_(std::move(socket)) {
  if (!pool_) {
    pool_ = memory::memoryManager()->addLeafPool();
  }
}

void RestSession::run() {
  doRead();
}

void RestSession::doRead() {
  auto self = shared_from_this();
  boost::beast::http::async_read(
      socket_,
      buffer_,
      req_,
      [self](boost::beast::error_code ec, std::size_t bytes_transferred) {
        self->onRead(ec, bytes_transferred);
      });
}

void RestSession::onRead(
    boost::beast::error_code ec,
    std::size_t bytes_transferred) {
  boost::ignore_unused(bytes_transferred);

  if (ec == boost::beast::http::error::end_of_stream) {
    return doClose();
  }

  if (ec) {
    LOG(ERROR) << "Read error: " << ec.message();
    return;
  }

  handleRequest(std::move(req_));
}

void RestSession::handleRequest(
    boost::beast::http::request<boost::beast::http::string_body> req) {
  res_.version(req.version());
  res_.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
  if (req.method() != boost::beast::http::verb::post) {
    res_.result(boost::beast::http::status::method_not_allowed);
    res_.set(boost::beast::http::field::content_type, "text/plain");
    res_.body() = fmt::format(
        "Only POST method is allowed. Method used: {}",
        std::string(req.method_string()));
    res_.prepare_payload();

    auto self = shared_from_this();
    boost::beast::http::async_write(
        socket_,
        res_,
        [self](boost::beast::error_code ec, std::size_t bytes_transferred) {
          self->onWrite(true, ec, bytes_transferred);
        });
    return;
  }

  auto contentTypeHeader = req[boost::beast::http::field::content_type];
  if (contentTypeHeader.empty() ||
      contentTypeHeader != "application/X-presto-pages") {
    res_.result(boost::beast::http::status::unsupported_media_type);
    res_.set(boost::beast::http::field::content_type, "text/plain");
    res_.body() = fmt::format(
        "Unsupported Content-Type: '{}'. Expecting 'application/X-presto-pages'.",
        std::string(contentTypeHeader));

    res_.prepare_payload();

    auto self = shared_from_this();
    boost::beast::http::async_write(
        socket_,
        res_,
        [self](boost::beast::error_code ec, std::size_t bytes_transferred) {
          self->onWrite(true, ec, bytes_transferred);
        });
    return;
  }

  auto acceptHeader = req[boost::beast::http::field::accept];
  if (acceptHeader.empty() || acceptHeader != "application/X-presto-pages") {
    res_.result(boost::beast::http::status::not_acceptable);
    res_.set(boost::beast::http::field::content_type, "text/plain");
    res_.body() = fmt::format(
        "Unsupported Accept header: '{}'. Expecting 'application/X-presto-pages'.",
        std::string(acceptHeader));
    res_.prepare_payload();

    auto self = shared_from_this();
    boost::beast::http::async_write(
        socket_,
        res_,
        [self](boost::beast::error_code ec, std::size_t bytes_transferred) {
          self->onWrite(true, ec, bytes_transferred);
        });
    return;
  }

  // Extract function name from path: /{functionName}
  std::string path = req.target();
  std::vector<std::string> pathComponents;
  folly::split('/', path, pathComponents);

  std::string functionName;
  if (pathComponents.size() <= 2) {
    functionName = pathComponents[1];
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
          self->onWrite(true, ec, bytes_transferred);
        });
    return;
  }

  try {
    serializer::presto::PrestoVectorSerde serde;
    auto inputBuffer = folly::IOBuf::copyBuffer(req.body());

    if (functionName == "remote_abs") {
      std::vector<std::string> argTypeNames = {"integer"};
      std::string returnTypeName = "integer";

      auto argType = type::fbhive::HiveTypeParser().parse(argTypeNames[0]);
      auto outType = type::fbhive::HiveTypeParser().parse(returnTypeName);

      auto inputVector =
          IOBufToRowVector(*inputBuffer, ROW({argType}), *pool_, &serde);
      VELOX_CHECK_EQ(
          inputVector->childrenSize(),
          1,
          "Expected exactly 1 column for function 'remote_abs'.");

      const auto numRows = inputVector->size();
      auto resultVector = BaseVector::create(argType, numRows, pool_.get());

      auto inputFlat = inputVector->childAt(0)->asFlatVector<int32_t>();
      auto outFlat = resultVector->asFlatVector<int32_t>();
      for (vector_size_t i = 0; i < numRows; ++i) {
        if (inputFlat->isNullAt(i)) {
          outFlat->setNull(i, true);
        } else {
          int32_t val = inputFlat->valueAt(i);
          outFlat->set(i, std::abs(val));
        }
      }

      auto outputRowVector = std::make_shared<RowVector>(
          pool_.get(),
          ROW({outType}),
          BufferPtr(),
          numRows,
          std::vector<VectorPtr>{resultVector});

      auto payload = rowVectorToIOBuf(
          outputRowVector, outputRowVector->size(), *pool_, &serde);

      res_.result(boost::beast::http::status::ok);
      res_.set(
          boost::beast::http::field::content_type,
          "application/X-presto-pages");
      res_.body() = payload.moveToFbString().toStdString();
      res_.prepare_payload();
      auto self = shared_from_this();
      boost::beast::http::async_write(
          socket_,
          res_,
          [self](boost::beast::error_code ec, std::size_t bytes_transferred) {
            self->onWrite(false, ec, bytes_transferred);
          });

    } else if (functionName == "remote_strlen") {
      std::vector<std::string> argTypeNames = {"varchar"};
      std::string returnTypeName = "integer";

      auto argType = type::fbhive::HiveTypeParser().parse(argTypeNames[0]);
      auto outType = type::fbhive::HiveTypeParser().parse(returnTypeName);

      auto inputVector =
          IOBufToRowVector(*inputBuffer, ROW({argType}), *pool_, &serde);

      VELOX_CHECK_EQ(
          inputVector->childrenSize(),
          1,
          "Expected exactly 1 column for function 'remote_strlen'.");

      const auto numRows = inputVector->size();
      auto resultVector = BaseVector::create(outType, numRows, pool_.get());

      auto inputFlat = inputVector->childAt(0)->asFlatVector<StringView>();
      auto outFlat = resultVector->asFlatVector<int32_t>();

      for (vector_size_t i = 0; i < numRows; ++i) {
        if (inputFlat->isNullAt(i)) {
          outFlat->setNull(i, true);
        } else {
          int32_t stringLen = inputFlat->valueAt(i).size();
          outFlat->set(i, stringLen);
        }
      }

      auto outputRowVector = std::make_shared<RowVector>(
          pool_.get(),
          ROW({outType}),
          BufferPtr(),
          numRows,
          std::vector<VectorPtr>{resultVector});

      auto payload = rowVectorToIOBuf(
          outputRowVector, outputRowVector->size(), *pool_, &serde);

      res_.result(boost::beast::http::status::ok);
      res_.set(
          boost::beast::http::field::content_type,
          "application/X-presto-pages");
      res_.body() = payload.moveToFbString().toStdString();
      res_.prepare_payload();
      auto self = shared_from_this();
      boost::beast::http::async_write(
          socket_,
          res_,
          [self](boost::beast::error_code ec, std::size_t bytes_transferred) {
            self->onWrite(false, ec, bytes_transferred);
          });

    } else if (functionName == "remote_trim") {
      std::vector<std::string> argTypeNames = {"varchar"};
      std::string returnTypeName = "varchar";

      auto argType = type::fbhive::HiveTypeParser().parse(argTypeNames[0]);
      auto outType = type::fbhive::HiveTypeParser().parse(returnTypeName);

      auto inputVector =
          IOBufToRowVector(*inputBuffer, ROW({argType}), *pool_, &serde);

      VELOX_CHECK_EQ(
          inputVector->childrenSize(),
          1,
          "Expected exactly 1 column for function 'remote_strlen'.");

      const auto numRows = inputVector->size();
      auto resultVector = BaseVector::create(outType, numRows, pool_.get());

      auto inputFlat = inputVector->childAt(0)->asFlatVector<StringView>();
      auto outFlat = resultVector->asFlatVector<StringView>();

      for (vector_size_t i = 0; i < numRows; ++i) {
        if (inputFlat->isNullAt(i)) {
          outFlat->setNull(i, true);
        } else {
          std::string result = inputFlat->valueAt(i).str();
          result.erase(
              std::remove_if(result.begin(), result.end(), ::isspace),
              result.end());
          outFlat->set(i, result.data());
        }
      }

      auto outputRowVector = std::make_shared<RowVector>(
          pool_.get(),
          ROW({outType}),
          BufferPtr(),
          numRows,
          std::vector<VectorPtr>{resultVector});

      auto payload = rowVectorToIOBuf(
          outputRowVector, outputRowVector->size(), *pool_, &serde);

      res_.result(boost::beast::http::status::ok);
      res_.set(
          boost::beast::http::field::content_type,
          "application/X-presto-pages");
      res_.body() = payload.moveToFbString().toStdString();
      res_.prepare_payload();
      auto self = shared_from_this();
      boost::beast::http::async_write(
          socket_,
          res_,
          [self](boost::beast::error_code ec, std::size_t bytes_transferred) {
            self->onWrite(false, ec, bytes_transferred);
          });

    } else if (functionName == "remote_divide") {
      std::vector<std::string> argTypeNames = {"double", "double"};
      std::string returnTypeName = "double";

      auto numType = type::fbhive::HiveTypeParser().parse(argTypeNames[0]);
      auto denType = type::fbhive::HiveTypeParser().parse(argTypeNames[1]);
      auto outType = type::fbhive::HiveTypeParser().parse(returnTypeName);

      auto rowType = ROW({numType, denType});
      auto inputVector =
          IOBufToRowVector(*inputBuffer, rowType, *pool_, &serde);

      VELOX_CHECK_EQ(
          inputVector->childrenSize(),
          2,
          "Expected exactly 2 columns for function 'remote_divide'.");

      const auto numRows = inputVector->size();

      auto resultVector = BaseVector::create(outType, numRows, pool_.get());

      auto inputFlat0 = inputVector->childAt(0)->asFlatVector<double>();
      auto inputFlat1 = inputVector->childAt(1)->asFlatVector<double>();
      auto outFlat = resultVector->asFlatVector<double>();

      for (vector_size_t i = 0; i < numRows; ++i) {
        if (inputFlat0->isNullAt(i) || inputFlat1->isNullAt(i)) {
          // If either operand is null, result is null
          outFlat->setNull(i, true);
        } else {
          double dividend = inputFlat0->valueAt(i);
          double divisor = inputFlat1->valueAt(i);

          if (divisor == 0.0) {
            outFlat->setNull(i, true);
          } else {
            outFlat->set(i, dividend / divisor);
          }
        }
      }

      auto outputRowVector = std::make_shared<RowVector>(
          pool_.get(),
          ROW({outType}),
          BufferPtr(),
          numRows,
          std::vector<VectorPtr>{resultVector});

      auto payload = rowVectorToIOBuf(
          outputRowVector, outputRowVector->size(), *pool_, &serde);

      res_.result(boost::beast::http::status::ok);
      res_.set(
          boost::beast::http::field::content_type,
          "application/X-presto-pages");
      res_.body() = payload.moveToFbString().toStdString();
      res_.prepare_payload();

      auto self = shared_from_this();
      boost::beast::http::async_write(
          socket_,
          res_,
          [self](boost::beast::error_code ec, std::size_t bytes_transferred) {
            self->onWrite(false, ec, bytes_transferred);
          });
    }

    else {
      // Function name doesn't match any known function
      VELOX_USER_FAIL(
          "Function '{}' is not available on the server.", functionName);
    }

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
          self->onWrite(true, ec, bytes_transferred);
        });
  }
}

void RestSession::onWrite(
    bool close,
    boost::beast::error_code ec,
    std::size_t bytes_transferred) {
  boost::ignore_unused(bytes_transferred);

  if (ec) {
    LOG(ERROR) << "Write error: " << ec.message();
    return;
  }

  if (close) {
    return doClose();
  }

  req_ = {};

  doRead();
}

void RestSession::doClose() {
  boost::beast::error_code ec;
  socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_send, ec);
}

RestListener::RestListener(
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

void RestListener::run() {
  doAccept();
}

void RestListener::doAccept() {
  acceptor_.async_accept(
      [self = shared_from_this()](
          boost::beast::error_code ec, boost::asio::ip::tcp::socket socket) {
        self->onAccept(ec, std::move(socket));
      });
}

void RestListener::onAccept(
    boost::beast::error_code ec,
    boost::asio::ip::tcp::socket socket) {
  if (ec) {
    LOG(ERROR) << "Accept error: " << ec.message();
  } else {
    std::make_shared<RestSession>(std::move(socket), functionPrefix_)->run();
  }
  doAccept();
}

} // namespace facebook::velox::functions
