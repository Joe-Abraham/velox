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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions;
using namespace facebook::velox::test;

namespace {

class EncodingBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  explicit EncodingBenchmark() : FunctionBenchmarkBase() {
    prestosql::registerAllScalarFunctions();

    // Set input schema.
    inputType_ = ROW({
        {"input", VARBINARY()},
    });

    // Generate input data.
    rowVector_ = makeRowVector(100);
  }

  RowVectorPtr makeRowVector(vector_size_t size) {
    std::vector<VectorPtr> children{
        BaseVector::create(VARBINARY(), size, pool())};
    auto flatVector = children[0]->asFlatVector<StringView>();
    for (vector_size_t i = 0; i < size; ++i) {
      flatVector->set(i, "The quick brown fox jumps over the lazy dog");
    }
    return std::make_shared<RowVector>(
        pool(), inputType_, nullptr, size, std::move(children));
  }

  // Runs `expression` `times` thousand times.
  size_t run(const std::string& expression, size_t times = 1'000) {
    folly::BenchmarkSuspender suspender;
    auto exprSet = compileExpression(expression, inputType_);
    suspender.dismiss();

    size_t count = 0;
    for (auto i = 0; i < times; i++) {
      auto result = evaluate(exprSet, rowVector_);
      count += result->size();
    }
    return count;
  }

 private:
  TypePtr inputType_;
  RowVectorPtr rowVector_;
};

std::unique_ptr<EncodingBenchmark> benchmark;

BENCHMARK(toBase64Encode) {
  benchmark->run("to_base64(input)");
}
BENCHMARK(toBase64Decode) {
  benchmark->run("from_base64(to_base64(input))");
}

BENCHMARK(toBase64EncodeCppCodec) {
  benchmark->run("to_base64cppcodec(input)");
}
BENCHMARK(toBase64DecodeCppCodec) {
  benchmark->run("from_base64cppcodec(to_base64cppcodec(input))");
}

BENCHMARK(toBase32EncodeCppCodec) {
  benchmark->run("to_base32(input)");
}
BENCHMARK(toBase32DecodeCppCodec) {
  benchmark->run("from_base32(to_base32(input))");
}

} // namespace

int main(int argc, char* argv[]) {
  folly::Init init{&argc, &argv};
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memory::MemoryManager::initialize({});
  benchmark = std::make_unique<EncodingBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
