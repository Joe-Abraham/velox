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

#include <boost/asio.hpp>
#include <folly/SocketAddress.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <cstdio>
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/CheckedArithmetic.h"
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/functions/prestosql/StringFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/remote/client/Remote.h"
#include "velox/functions/remote/server/RemoteFunctionRestService.h"

using ::facebook::velox::test::assertEqualVectors;

namespace facebook::velox::functions {
namespace {

class RemoteFunctionRestTest
    : public test::FunctionBaseTest,
      public testing::WithParamInterface<remote::PageFormat> {
 public:
  void SetUp() override {
    initializeServer();
    registerRemoteFunctions();
  }

  // Registers a few remote functions to be used in this test.
  void registerRemoteFunctions() const {
    RemoteVectorFunctionMetadata metadata;
    metadata.serdeFormat = GetParam();
    metadata.location = location_;

    auto absSignature = {exec::FunctionSignatureBuilder()
                             .returnType("integer")
                             .argumentType("integer")
                             .build()};
    registerRemoteFunction("remote_abs", absSignature, metadata);

    auto plusSignatures = {exec::FunctionSignatureBuilder()
                               .returnType("bigint")
                               .argumentType("bigint")
                               .argumentType("bigint")
                               .build()};
    registerRemoteFunction("remote_plus", plusSignatures, metadata);

    RemoteVectorFunctionMetadata wrongMetadata = metadata;
    wrongMetadata.location = folly::SocketAddress(); // empty address.
    registerRemoteFunction("remote_wrong_port", plusSignatures, wrongMetadata);

    auto divSignatures = {exec::FunctionSignatureBuilder()
                              .returnType("double")
                              .argumentType("double")
                              .argumentType("double")
                              .build()};
    registerRemoteFunction("remote_divide", divSignatures, metadata);

    auto substrSignatures = {exec::FunctionSignatureBuilder()
                                 .returnType("varchar")
                                 .argumentType("varchar")
                                 .argumentType("integer")
                                 .build()};
    registerRemoteFunction("remote_substr", substrSignatures, metadata);

    // Registers the actual function under a different prefix. This is only
    // needed for tests since the HTTP service runs in the same process.
    registerFunction<AbsFunction, int32_t, int32_t>(
        {remotePrefix_ + ".remote_abs"});
    registerFunction<PlusFunction, int64_t, int64_t, int64_t>(
        {remotePrefix_ + ".remote_plus"});
    registerFunction<CheckedDivideFunction, double, double, double>(
        {remotePrefix_ + ".remote_divide"});
    registerFunction<SubstrFunction, Varchar, Varchar, int32_t>(
        {remotePrefix_ + ".remote_substr"});
  }

  void initializeServer() {
    // Adjusted for Boost.Beast server; the server is started in the main
    // thread.

    // Start the server in a separate thread
    serverThread_ = std::make_unique<std::thread>([this]() {
      std::string serviceHost = "127.0.0.1";
      int32_t servicePort = 8321;
      std::string functionPrefix = remotePrefix_;

      boost::asio::io_context ioc{1};

      std::make_shared<listener>(
          ioc,
          boost::asio::ip::tcp::endpoint(
              boost::asio::ip::make_address(serviceHost), servicePort),
          functionPrefix)
          ->run();

      ioc.run();
    });

    VELOX_CHECK(waitForRunning(), "Unable to initialize HTTP server.");
    LOG(INFO) << "HTTP server is up and running at " << location_;
  }

  ~RemoteFunctionRestTest() override {
    // Signal the server thread to stop
    serverThread_->detach();
    LOG(INFO) << "HTTP server stopped.";
  }

 private:
  bool waitForRunning() const {
    for (size_t i = 0; i < 100; ++i) {
      using boost::asio::ip::tcp;
      boost::asio::io_context io_context;

      tcp::socket socket(io_context);
      tcp::resolver resolver(io_context);

      try {
        boost::asio::connect(
            socket, resolver.resolve("127.0.0.1", std::to_string(8321)));
        return true;
      } catch (std::exception& e) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }
    }
    return false;
  }

  std::unique_ptr<std::thread> serverThread_;

  std::string location_{("http://127.0.0.1:8321")};
  const std::string remotePrefix_{"remote"};
};

TEST_P(RemoteFunctionRestTest, absolute) {
  auto inputVector = makeFlatVector<int32_t>({-10, -20});
  auto results = evaluate<SimpleVector<int32_t>>(
      "remote_abs(c0)", makeRowVector({inputVector}));

  auto expected = makeFlatVector<int32_t>({10, 20});
  assertEqualVectors(expected, results);
}

TEST_P(RemoteFunctionRestTest, simple) {
  auto inputVector = makeFlatVector<int64_t>({1, 2, 3, 4, 5});
  auto results = evaluate<SimpleVector<int64_t>>(
      "remote_plus(c0, c0)", makeRowVector({inputVector}));

  auto expected = makeFlatVector<int64_t>({2, 4, 6, 8, 10});
  assertEqualVectors(expected, results);
}

TEST_P(RemoteFunctionRestTest, string) {
  auto inputVector =
      makeFlatVector<StringView>({"hello", "my", "remote", "world"});
  auto inputVector1 = makeFlatVector<int32_t>({2, 1, 3, 5});
  auto results = evaluate<SimpleVector<StringView>>(
      "remote_substr(c0, c1)", makeRowVector({inputVector, inputVector1}));

  auto expected = makeFlatVector<StringView>({"ello", "my", "mote", "d"});
  assertEqualVectors(expected, results);
}

TEST_P(RemoteFunctionRestTest, connectionError) {
  auto inputVector = makeFlatVector<int64_t>({1, 2, 3, 4, 5});
  auto func = [&]() {
    evaluate<SimpleVector<int64_t>>(
        "remote_wrong_port(c0, c0)", makeRowVector({inputVector}));
  };

  // Check it throws and that the exception has the "connection refused"
  // substring.
  EXPECT_THROW(func(), VeloxRuntimeError);
  try {
    func();
  } catch (const VeloxRuntimeError& e) {
    EXPECT_THAT(e.message(), testing::HasSubstr("Channel is !good()"));
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    RemoteFunctionRestTestFixture,
    RemoteFunctionRestTest,
    ::testing::Values(remote::PageFormat::PRESTO_PAGE));

} // namespace
} // namespace facebook::velox::functions

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
