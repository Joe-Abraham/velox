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

#include <folly/io/IOBuf.h>
#include <memory>
#include <string>

namespace facebook::velox::functions {

class HttpClient {
 public:
  virtual ~HttpClient() = default;

  virtual std::unique_ptr<folly::IOBuf> performCurlRequest(
      const std::string& url,
      std::unique_ptr<folly::IOBuf> requestPayload) = 0;
};

class RestClient : public HttpClient {
 public:
  std::unique_ptr<folly::IOBuf> performCurlRequest(
      const std::string& url,
      std::unique_ptr<folly::IOBuf> requestPayload) override;
};

std::unique_ptr<HttpClient> getRestClient();

} // namespace facebook::velox::functions
