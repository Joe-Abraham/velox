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

#include "velox/functions/remote/client/RestClient.h"

#include <curl/curl.h>
#include <folly/io/IOBufQueue.h>
#include <simdjson.h>

#include "velox/common/base/Exceptions.h"

using namespace folly;
namespace facebook::velox::functions {
namespace {
size_t readCallback(char* dest, size_t size, size_t nmemb, void* userp) {
  auto* inputBufQueue = static_cast<IOBufQueue*>(userp);
  size_t bufferSize = size * nmemb;
  size_t totalCopied = 0;

  while (totalCopied < bufferSize && !inputBufQueue->empty()) {
    auto buf = inputBufQueue->front();
    size_t remainingSize = bufferSize - totalCopied;
    size_t copySize = std::min(remainingSize, buf->length());
    std::memcpy(dest + totalCopied, buf->data(), copySize);
    totalCopied += copySize;
    inputBufQueue->pop_front();
  }

  return totalCopied;
}
size_t writeCallback(char* ptr, size_t size, size_t nmemb, void* userdata) {
  auto* outputBuf = static_cast<IOBufQueue*>(userdata);
  size_t total_size = size * nmemb;
  auto buf = IOBuf::copyBuffer(ptr, total_size);
  outputBuf->append(std::move(buf));
  return total_size;
}
} // namespace

std::unique_ptr<IOBuf> RestClient::performCurlRequest(
    const std::string& fullUrl,
    std::unique_ptr<IOBuf> requestPayload) {
  try {
    IOBufQueue inputBufQueue(IOBufQueue::cacheChainLength());
    inputBufQueue.append(std::move(requestPayload));

    CURL* curl = curl_easy_init();
    if (!curl) {
      VELOX_FAIL(fmt::format(
          "Error initializing CURL: {}",
          curl_easy_strerror(CURLE_FAILED_INIT)));
    }

    curl_easy_setopt(curl, CURLOPT_URL, fullUrl.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_READFUNCTION, readCallback);
    curl_easy_setopt(curl, CURLOPT_READDATA, &inputBufQueue);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);

    IOBufQueue outputBuf(IOBufQueue::cacheChainLength());
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &outputBuf);
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);

    struct curl_slist* headers = nullptr;
    headers =
        curl_slist_append(headers, "Content-Type: application/X-presto-pages");
    headers = curl_slist_append(headers, "Accept: application/X-presto-pages");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    curl_easy_setopt(
        curl,
        CURLOPT_POSTFIELDSIZE,
        static_cast<long>(inputBufQueue.chainLength()));

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      curl_slist_free_all(headers);
      curl_easy_cleanup(curl);
      VELOX_FAIL(fmt::format(
          "Error communicating with server: {}\nURL: {}\nCURL Error: {}",
          curl_easy_strerror(res),
          fullUrl.c_str(),
          curl_easy_strerror(res)));
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    return outputBuf.move();

  } catch (const std::exception& e) {
    VELOX_FAIL(fmt::format("Exception during CURL request: {}", e.what()));
  }
}

std::unique_ptr<HttpClient> getRestClient() {
  return std::make_unique<RestClient>();
}

} // namespace facebook::velox::functions
