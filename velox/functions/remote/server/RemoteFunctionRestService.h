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

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include "velox/common/memory/Memory.h"

namespace facebook::velox::functions {

class session : public std::enable_shared_from_this<session> {
 public:
  session(boost::asio::ip::tcp::socket socket, std::string functionPrefix);

  void run();

 private:
  void do_read();

  void on_read(boost::beast::error_code ec, std::size_t bytes_transferred);

  void handle_request(
      boost::beast::http::request<boost::beast::http::string_body> req);

  void on_write(
      bool close,
      boost::beast::error_code ec,
      std::size_t bytes_transferred);

  void do_close();

  boost::asio::ip::tcp::socket socket_;
  boost::beast::flat_buffer buffer_;
  std::string functionPrefix_;
  boost::beast::http::request<boost::beast::http::string_body> req_;
  boost::beast::http::response<boost::beast::http::string_body> res_;
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};

class listener : public std::enable_shared_from_this<listener> {
 public:
  listener(
      boost::asio::io_context& ioc,
      boost::asio::ip::tcp::endpoint endpoint,
      std::string functionPrefix);

  void run();

 private:
  void do_accept();

  void on_accept(
      boost::beast::error_code ec,
      boost::asio::ip::tcp::socket socket);

  boost::asio::io_context& ioc_;
  boost::asio::ip::tcp::acceptor acceptor_;
  std::string functionPrefix_;
};

} // namespace facebook::velox::functions
