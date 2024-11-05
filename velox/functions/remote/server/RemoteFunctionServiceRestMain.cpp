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
#include <folly/init/Init.h>
#include "RemoteFunctionRestService.h"
#include "velox/common/memory/Memory.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

DEFINE_string(service_host, "127.0.0.1", "Host to bind the service to");

DEFINE_int32(service_port, 8321, "Port to bind the service to");

DEFINE_string(
    function_prefix,
    "remote.schema",
    "Prefix to be added to the functions being registered");

using namespace ::facebook::velox;

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv);
  FLAGS_logtostderr = true;
  memory::initializeMemoryManager({});

  functions::prestosql::registerAllScalarFunctions(FLAGS_function_prefix);
  boost::asio::io_context ioc{1};

  std::make_shared<functions::listener>(
      ioc,
      boost::asio::ip::tcp::endpoint(
          boost::asio::ip::make_address(FLAGS_service_host),
          FLAGS_service_port),
      FLAGS_function_prefix)
      ->run();

  ioc.run();

  return 0;
}
