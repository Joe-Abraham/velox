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

#include "velox/functions/prestosql/types/JsonCastOperator.h"

#include "velox/expression/PeeledEncoding.h"
#include "velox/expression/VectorWriters.h"
#include "velox/functions/lib/RowsTranslationUtil.h"
#include "velox/functions/prestosql/json/JsonStringUtil.h"
#include "velox/functions/prestosql/json/SIMDJsonUtil.h"
#include "velox/functions/prestosql/types/JsonType.h"

namespace facebook::velox {
namespace {
template <typename T, bool legacyCast>
void generateJsonTyped(
    const SimpleVector<T>& input,
    int row,
    std::string& result,
    const TypePtr& type,
    const std::shared_ptr<exec::CastHooks>& hooks) {
  auto value = input.valueAt(row);

  if constexpr (std::is_same_v<T, StringView>) {
    size_t resultSize = normalizedSizeForJsonCast(value.data(), value.size());
    result.resize(resultSize + 2);
    result.data()[0] = '"';
    normalizeForJsonCast(value.data(), value.size(), result.data() + 1);
    result.data()[resultSize + 1] = '"';
  } else if constexpr (std::is_same_v<T, UnknownValue>) {
    VELOX_FAIL(
        "Casting UNKNOWN to JSON: Vectors of UNKNOWN type should not contain non-null rows");
  } else {
    if constexpr (std::is_same_v<T, bool>) {
      result.append(value ? "true" : "false");
    } else if constexpr (
        std::is_same_v<T, double> || std::is_same_v<T, float>) {
      if constexpr (!legacyCast) {
        if (FOLLY_UNLIKELY(std::isinf(value) || std::isnan(value))) {
          result.append(fmt::format(
              "\"{}\"",
              util::Converter<TypeKind::VARCHAR>::tryCast(value).value()));
        } else {
          result.append(
              util::Converter<TypeKind::VARCHAR>::tryCast(value).value());
        }
      } else {
        folly::toAppend<std::string, T>(value, &result);
      }
    } else if constexpr (std::is_same_v<T, Timestamp>) {
      std::string buffer;
      if (hooks) {
        Timestamp inputValue = value;
        const auto& options = hooks->timestampToStringOptions();
        if (options.timeZone) {
          inputValue.toTimezone(*(options.timeZone));
        }
        buffer.resize(getMaxStringLength(options));
        const auto stringView =
            Timestamp::tsToStringView(inputValue, options, buffer.data());
        buffer.resize(stringView.size());
      } else {
        buffer = std::to_string(value);
      }
      result.reserve(buffer.size() + 2);
      result.append("\"");
      result.append(buffer);
      result.append("\"");
    } else if (type->isDate()) {
      std::string stringValue = DATE()->toString(value);
      result.reserve(stringValue.size() + 2);
      result.append("\"");
      result.append(stringValue);
      result.append("\"");
    } else if (type->isDecimal()) {
      result.append(DecimalUtil::toString(value, type));
    } else {
      folly::toAppend<std::string, T>(value, &result);
    }
  }
}

template <typename T, bool legacyCast>
void generateJsonNonKeyTyped(
    const SimpleVector<T>& inputVector,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    const std::shared_ptr<exec::CastHooks>& hooks) {
  std::string result;
  context.applyToSelectedNoThrow(rows, [&](auto row) {
    if (inputVector.isNullAt(row)) {
      flatResult.set(row, "null");
    } else {
      result.clear();
      generateJsonTyped<T, legacyCast>(
          inputVector, row, result, inputVector.type(), hooks);

      flatResult.set(row, StringView{result});
    }
  });
}

template <typename T, bool legacyCast>
void generateJsonKeyTyped(
    const SimpleVector<T>& inputVector,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    const std::shared_ptr<exec::CastHooks>& hooks) {
  std::string result;
  context.applyToSelectedNoThrow(rows, [&](auto row) {
    if (inputVector.isNullAt(row)) {
      VELOX_USER_FAIL("Map keys cannot be null.");
    } else {
      result.clear();

      if constexpr (!std::is_same_v<T, StringView>) {
        result.append("\"");
      }

      generateJsonTyped<T, legacyCast>(
          inputVector, row, result, inputVector.type(), hooks);

      if constexpr (!std::is_same_v<T, StringView>) {
        result.append("\"");
      }

      flatResult.set(row, StringView{result});
    }
  });
}

// Casts primitive-type input vectors to Json type.
template <
    TypeKind kind,
    typename std::enable_if_t<TypeTraits<kind>::isPrimitiveType, int> = 0>
void castToJson(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    const std::shared_ptr<exec::CastHooks>& hooks,
    bool isMapKey = false) {
  using T = typename TypeTraits<kind>::NativeType;

  // input is guaranteed to be in flat or constant encodings when passed in.
  auto inputVector = input.as<SimpleVector<T>>();

  bool legacyCast = context.execCtx()->queryCtx()->queryConfig().isLegacyCast();

  if (FOLLY_LIKELY(!legacyCast)) {
    if (!isMapKey) {
      generateJsonNonKeyTyped<T, false>(
          *inputVector, context, rows, flatResult, hooks);
    } else {
      generateJsonKeyTyped<T, false>(
          *inputVector, context, rows, flatResult, hooks);
    }
  } else {
    if (!isMapKey) {
      generateJsonNonKeyTyped<T, true>(
          *inputVector, context, rows, flatResult, hooks);
    } else {
      generateJsonKeyTyped<T, true>(
          *inputVector, context, rows, flatResult, hooks);
    }
  }
}

// Forward declaration.
void castToJsonFromArray(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    const std::shared_ptr<exec::CastHooks>& hooks);

void castToJsonFromMap(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    const std::shared_ptr<exec::CastHooks>& hooks);

void castToJsonFromRow(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    const std::shared_ptr<exec::CastHooks>& hooks);

// Casts complex-type input vectors to Json type.
template <
    TypeKind kind,
    typename std::enable_if_t<!TypeTraits<kind>::isPrimitiveType, int> = 0>
void castToJson(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    const std::shared_ptr<exec::CastHooks>& hooks,
    bool isMapKey = false) {
  VELOX_CHECK(
      !isMapKey, "Casting map with complex key type to JSON is not supported");

  if constexpr (kind == TypeKind::ARRAY) {
    castToJsonFromArray(input, context, rows, flatResult, hooks);
  } else if constexpr (kind == TypeKind::MAP) {
    castToJsonFromMap(input, context, rows, flatResult, hooks);
  } else if constexpr (kind == TypeKind::ROW) {
    castToJsonFromRow(input, context, rows, flatResult, hooks);
  } else {
    VELOX_FAIL(
        "Casting {} to JSON is not supported.", input.type()->toString());
  }
}

// Helper struct representing the Json vector of input.
struct AsJson {
  AsJson(
      exec::EvalCtx& context,
      const VectorPtr& input,
      const SelectivityVector& rows,
      const BufferPtr& elementToTopLevelRows,
      const std::shared_ptr<exec::CastHooks>& hooks,
      bool isMapKey = false,
      std::optional<std::string> fieldName = std::nullopt)
      : decoded_(context), fieldName_(std::move(fieldName)) {
    VELOX_CHECK(rows.hasSelections());

    exec::EvalErrorsPtr oldErrors;
    context.swapErrors(oldErrors);
    if (isJsonType(input->type())) {
      json_ = input;
    } else {
      if (!exec::PeeledEncoding::isPeelable(input->encoding())) {
        doCast(context, input, rows, isMapKey, json_, hooks);
      } else {
        exec::withContextSaver([&](exec::ContextSaver& saver) {
          exec::LocalSelectivityVector newRowsHolder(*context.execCtx());

          exec::LocalDecodedVector localDecoded(context);
          std::vector<VectorPtr> peeledVectors;
          auto peeledEncoding = exec::PeeledEncoding::peel(
              {input}, rows, localDecoded, true, peeledVectors);
          VELOX_CHECK_EQ(peeledVectors.size(), 1);
          auto newRows =
              peeledEncoding->translateToInnerRows(rows, newRowsHolder);
          // Save context and set the peel.
          context.saveAndReset(saver, rows);
          context.setPeeledEncoding(peeledEncoding);

          doCast(context, peeledVectors[0], *newRows, isMapKey, json_, hooks);
          json_ = context.getPeeledEncoding()->wrap(
              json_->type(), context.pool(), json_, rows);
        });
      }
    }
    decoded_.get()->decode(*json_, rows);
    jsonStrings_ = decoded_->base()->as<SimpleVector<StringView>>();

    if (isMapKey && decoded_->mayHaveNulls()) {
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        if (decoded_->isNullAt(row)) {
          VELOX_USER_FAIL("Cannot cast map with null keys to JSON.");
        }
      });
    }
    combineErrors(context, rows, elementToTopLevelRows, oldErrors);
  }

  StringView at(vector_size_t i) const {
    return jsonStrings_->valueAt(decoded_->index(i));
  }

  // Returns the length of the json string of the value at i, when this
  // value will be inlined as an element in the json string of an array, map, or
  // row.
  vector_size_t lengthAt(vector_size_t i) const {
    if (decoded_->isNullAt(i)) {
      // Null values are inlined as "null".
      return 4;
    } else {
      // If we have field names, we need to add the field name to the length.
      if (fieldName_.has_value()) {
        return fieldName_->size() + 3 + this->at(i).size();
      }
      return this->at(i).size();
    }
  }

  // Appends the json string of the value at i to a string writer.
  void append(vector_size_t i, exec::StringWriter& proxy) const {
    if (fieldName_.has_value()) {
      proxy.append(fmt::format("\"{}\":", fieldName_.value()));
    }

    if (decoded_->isNullAt(i)) {
      proxy.append("null");
    } else {
      proxy.append(this->at(i));
    }
  }

 private:
  void doCast(
      exec::EvalCtx& context,
      const VectorPtr& input,
      const SelectivityVector& baseRows,
      bool isMapKey,
      VectorPtr& result,
      const std::shared_ptr<exec::CastHooks>& hooks) {
    context.ensureWritable(baseRows, JSON(), result);
    auto flatJsonStrings = result->as<FlatVector<StringView>>();

    VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
        castToJson,
        input->typeKind(),
        *input,
        context,
        baseRows,
        *flatJsonStrings,
        hooks,
        isMapKey);
  }

  // Combine exceptions in oldErrors into context.errors_ with a transformation
  // of rows mapping provided by elementToTopLevelRows. If there are exceptions
  // at the same row in both context.errors_ and oldErrors, the one in oldErrors
  // remains. elementToTopLevelRows can be a nullptr, meaning that the rows in
  // context.errors_ correspond to rows in oldErrors exactly.
  void combineErrors(
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const BufferPtr& elementToTopLevelRows,
      exec::EvalErrorsPtr& oldErrors) {
    if (context.errors()) {
      if (elementToTopLevelRows) {
        context.addElementErrorsToTopLevel(
            rows, elementToTopLevelRows, oldErrors);
      } else {
        context.addErrors(rows, *context.errorsPtr(), oldErrors);
      }
    }
    context.swapErrors(oldErrors);
  }

  exec::LocalDecodedVector decoded_;
  VectorPtr json_;
  const SimpleVector<StringView>* jsonStrings_;
  std::optional<std::string> fieldName_;
};

void castToJsonFromArray(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    const std::shared_ptr<exec::CastHooks>& hooks) {
  // input is guaranteed to be in flat encoding when passed in.
  auto inputArray = input.as<ArrayVector>();

  auto elements = inputArray->elements();
  auto elementsRows =
      functions::toElementRows(elements->size(), rows, inputArray);
  if (!elementsRows.hasSelections()) {
    // All arrays are null or empty.
    context.applyToSelectedNoThrow(rows, [&](auto row) {
      if (inputArray->isNullAt(row)) {
        flatResult.set(row, "null");
      } else {
        VELOX_CHECK_EQ(
            inputArray->sizeAt(row),
            0,
            "All arrays are expected to be null or empty");
        flatResult.set(row, "[]");
      }
    });
    return;
  }

  auto elementToTopLevelRows = functions::getElementToTopLevelRows(
      elements->size(), rows, inputArray, context.pool());
  AsJson elementsAsJson{
      context, elements, elementsRows, elementToTopLevelRows, hooks};

  // Estimates an upperbound of the total length of all Json strings for the
  // input according to the length of all elements Json strings and the
  // delimiters to be added.
  size_t elementsStringSize = 0;
  context.applyToSelectedNoThrow(rows, [&](auto row) {
    if (inputArray->isNullAt(row)) {
      // "null" will be inlined in the StringView.
      return;
    }

    auto offset = inputArray->offsetAt(row);
    auto size = inputArray->sizeAt(row);
    for (auto i = offset, end = offset + size; i < end; ++i) {
      elementsStringSize += elementsAsJson.lengthAt(i);
    }

    // Extra length for commas and brackets.
    elementsStringSize += size > 0 ? size + 1 : 2;
  });

  flatResult.getBufferWithSpace(elementsStringSize);

  // Constructs the Json string of each array from Json strings of its elements.
  context.applyToSelectedNoThrow(rows, [&](auto row) {
    if (inputArray->isNullAt(row)) {
      flatResult.set(row, "null");
      return;
    }

    auto offset = inputArray->offsetAt(row);
    auto size = inputArray->sizeAt(row);

    auto proxy = exec::StringWriter(&flatResult, row);

    proxy.append("["_sv);
    for (int i = offset, end = offset + size; i < end; ++i) {
      if (i > offset) {
        proxy.append(","_sv);
      }
      elementsAsJson.append(i, proxy);
    }
    proxy.append("]"_sv);

    proxy.finalize();
  });
}

void castToJsonFromMap(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    const std::shared_ptr<exec::CastHooks>& hooks) {
  // input is guaranteed to be in flat encoding when passed in.
  auto inputMap = input.as<MapVector>();

  auto mapKeys = inputMap->mapKeys();
  auto mapValues = inputMap->mapValues();
  auto elementsRows = functions::toElementRows(mapKeys->size(), rows, inputMap);
  if (!elementsRows.hasSelections()) {
    // All maps are null or empty.
    context.applyToSelectedNoThrow(rows, [&](auto row) {
      if (inputMap->isNullAt(row)) {
        flatResult.set(row, "null");
      } else {
        VELOX_CHECK_EQ(
            inputMap->sizeAt(row),
            0,
            "All maps are expected to be null or empty");
        flatResult.set(row, "{}");
      }
    });
    return;
  }

  // Is map key and value unknown ?
  if (mapKeys->type()->kind() == TypeKind::UNKNOWN &&
      mapValues->type()->kind() == TypeKind::UNKNOWN) {
    context.applyToSelectedNoThrow(
        rows, [&](auto row) { flatResult.set(row, "{}"); });
    return;
  }

  auto elementToTopLevelRows = functions::getElementToTopLevelRows(
      mapKeys->size(), rows, inputMap, context.pool());
  // Maps with unsupported key types should have already been rejected by
  // JsonCastOperator::isSupportedType() beforehand.
  AsJson keysAsJson{
      context, mapKeys, elementsRows, elementToTopLevelRows, hooks, true};
  AsJson valuesAsJson{
      context, mapValues, elementsRows, elementToTopLevelRows, hooks};

  // Estimates an upperbound of the total length of all Json strings for the
  // input according to the length of all elements Json strings and the
  // delimiters to be added.
  size_t elementsStringSize = 0;
  context.applyToSelectedNoThrow(rows, [&](auto row) {
    if (inputMap->isNullAt(row)) {
      // "null" will be inlined in the StringView.
      return;
    }

    auto offset = inputMap->offsetAt(row);
    auto size = inputMap->sizeAt(row);
    for (auto i = offset, end = offset + size; i < end; ++i) {
      // The construction of keysAsJson ensured there is no null in keysAsJson.
      elementsStringSize += keysAsJson.at(i).size() + valuesAsJson.lengthAt(i);
    }

    // Extra length for commas, semicolons, and curly braces.
    elementsStringSize += size > 0 ? size * 2 + 1 : 2;
  });

  flatResult.getBufferWithSpace(elementsStringSize);

  // Constructs the Json string of each map from Json strings of its keys and
  // values.
  std::vector<std::pair<StringView, vector_size_t>> sortedKeys;
  context.applyToSelectedNoThrow(rows, [&](auto row) {
    if (inputMap->isNullAt(row)) {
      flatResult.set(row, "null");
      return;
    }

    auto offset = inputMap->offsetAt(row);
    auto size = inputMap->sizeAt(row);

    // Sort entries by keys in each map.
    sortedKeys.clear();
    for (int i = offset, end = offset + size; i < end; ++i) {
      sortedKeys.push_back(std::make_pair(keysAsJson.at(i), i));
    }
    std::sort(sortedKeys.begin(), sortedKeys.end());

    auto proxy = exec::StringWriter(&flatResult, row);

    proxy.append("{"_sv);
    for (auto it = sortedKeys.begin(); it != sortedKeys.end(); ++it) {
      if (it != sortedKeys.begin()) {
        proxy.append(","_sv);
      }
      proxy.append(it->first);
      proxy.append(":"_sv);
      valuesAsJson.append(it->second, proxy);
    }
    proxy.append("}"_sv);

    proxy.finalize();
  });
}

void castToJsonFromRow(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    const std::shared_ptr<exec::CastHooks>& hooks) {
  using NameJsonPair = std::pair<std::string, AsJson>;
  // input is guaranteed to be in flat encoding when passed in.
  VELOX_CHECK_EQ(input.encoding(), VectorEncoding::Simple::ROW);
  auto inputRow = input.as<RowVector>();
  auto childrenSize = inputRow->childrenSize();
  auto fieldNamesInJsonCastEnabled = context.execCtx()
                                         ->queryCtx()
                                         ->queryConfig()
                                         .isFieldNamesInJsonCastEnabled();

  // Estimates an upperbound of the total length of all Json strings for the
  // input according to the length of all children Json strings and the
  // delimiters to be added.
  size_t childrenStringSize = 0;
  std::vector<NameJsonPair> jsonChildren;

  for (int i = 0; i < childrenSize; ++i) {
    auto name = inputRow->type()->asRow().nameOf(i);
    std::optional<std::string> fieldName =
        fieldNamesInJsonCastEnabled ? std::optional{name} : std::nullopt;

    jsonChildren.emplace_back(
        name,
        AsJson{
            context,
            inputRow->childAt(i),
            rows,
            nullptr,
            hooks,
            false,
            std::move(fieldName)});

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      if (inputRow->isNullAt(row)) {
        // "null" will be inlined in the StringView.
        return;
      }
      childrenStringSize += jsonChildren[i].second.lengthAt(row);
    });
  }

  // Extra length for commas and brackets.
  childrenStringSize +=
      rows.countSelected() * (childrenSize > 0 ? childrenSize + 1 : 2);
  flatResult.getBufferWithSpace(childrenStringSize);

  // Make sure to sort the children based on their field names if
  // fieldNamesInJsonCastEnabled is true. This is to make sure the output is
  // canoncialized.

  if (fieldNamesInJsonCastEnabled) {
    std::sort(
        jsonChildren.begin(),
        jsonChildren.end(),
        [](const NameJsonPair& a, const NameJsonPair& b) {
          return a.first < b.first;
        });
  }

  // Constructs Json string of each row from Json strings of its children.
  context.applyToSelectedNoThrow(rows, [&](auto row) {
    if (inputRow->isNullAt(row)) {
      flatResult.set(row, "null");
      return;
    }

    auto proxy = exec::StringWriter(&flatResult, row);

    if (fieldNamesInJsonCastEnabled) {
      proxy.append("{"_sv);
    } else {
      proxy.append("["_sv);
    }

    for (int i = 0; i < jsonChildren.size(); ++i) {
      if (i > 0) {
        proxy.append(","_sv);
      }
      jsonChildren[i].second.append(row, proxy);
    }

    if (fieldNamesInJsonCastEnabled) {
      proxy.append("}"_sv);
    } else {
      proxy.append("]"_sv);
    }

    proxy.finalize();
  });
}

template <typename T>
simdjson::simdjson_result<T> fromString(const std::string_view& s) {
  auto result = folly::tryTo<T>(s);
  if (result.hasError()) {
    return simdjson::INCORRECT_TYPE;
  }

  if constexpr (std::is_floating_point_v<T>) {
    // Only "NaN" is allowed to be converted to NaN.  "nan" is not allowed.
    if (FOLLY_UNLIKELY(std::isnan(*result))) {
      if (s != "NaN" && s != "-NaN") {
        return simdjson::INCORRECT_TYPE;
      }
    }
  }

  return std::move(*result);
}

// Write x to writer if x is in the range of writer type `To'.  Only the
// following cases are supported:
//
// Signed Integer -> Signed Integer
// Float | Double -> Float | Double | Signed Integer
template <typename To, typename From>
simdjson::error_code convertIfInRange(From x, exec::GenericWriter& writer) {
  static_assert(std::is_signed_v<From> && std::is_signed_v<To>);
  if constexpr (std::is_integral_v<To> == std::is_integral_v<From>) {
    if constexpr (sizeof(To) < sizeof(From)) {
      constexpr From kMin = std::numeric_limits<To>::lowest();
      constexpr From kMax = std::numeric_limits<To>::max();
      if (!(kMin <= x && x <= kMax)) {
        return simdjson::NUMBER_OUT_OF_RANGE;
      }
    }
    writer.castTo<To>() = x;
    return simdjson::SUCCESS;
  } else {
    static_assert(std::is_integral_v<To> && !std::is_integral_v<From>);
    // Upper/lower bound values that could be accurately represented in both
    // int64_t and double types.  Same values are used by
    // folly::constexpr_clamp_cast.
    constexpr double kMin = -9223372036854774784.0;
    constexpr double kMax = 9223372036854774784.0;
    if (!(kMin <= x && x <= kMax)) {
      return simdjson::NUMBER_OUT_OF_RANGE;
    }

    // Need to round to nearest integer to be conformant with Java.
    simdjson::error_code err{simdjson::NUMBER_OUT_OF_RANGE};
    folly::tryTo<To>(std::round(x)).then([&err, &writer](To y) {
      err = convertIfInRange<To, int64_t>(y, writer);
    });

    return err;
  }
}

template <TypeKind kind>
simdjson::error_code appendMapKey(
    const std::string_view& value,
    exec::GenericWriter& writer) {
  using T = typename TypeTraits<kind>::NativeType;
  if constexpr (std::is_same_v<T, void>) {
    return simdjson::INCORRECT_TYPE;
  } else {
    SIMDJSON_ASSIGN_OR_RAISE(writer.castTo<T>(), fromString<T>(value));
    return simdjson::SUCCESS;
  }
}

template <>
simdjson::error_code appendMapKey<TypeKind::VARCHAR>(
    const std::string_view& value,
    exec::GenericWriter& writer) {
  writer.castTo<Varchar>().append(value);
  return simdjson::SUCCESS;
}

template <>
simdjson::error_code appendMapKey<TypeKind::VARBINARY>(
    const std::string_view& /*value*/,
    exec::GenericWriter& /*writer*/) {
  return simdjson::INCORRECT_TYPE;
}

template <>
simdjson::error_code appendMapKey<TypeKind::TIMESTAMP>(
    const std::string_view& /*value*/,
    exec::GenericWriter& /*writer*/) {
  return simdjson::INCORRECT_TYPE;
}

template <typename Input>
struct CastFromJsonTypedImpl {
  template <TypeKind kind>
  static simdjson::error_code apply(Input input, exec::GenericWriter& writer) {
    return KindDispatcher<kind>::apply(input, writer);
  }

 private:
  // Dummy is needed because full/explicit specialization is not allowed inside
  // class.
  template <TypeKind kind, typename Dummy = void>
  struct KindDispatcher {
    static simdjson::error_code apply(Input, exec::GenericWriter&) {
      VELOX_NYI(
          "Casting from JSON to {} is not supported.", TypeTraits<kind>::name);
      return simdjson::error_code::UNEXPECTED_ERROR; // Make compiler happy.
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::VARCHAR, Dummy> {
    static simdjson::error_code apply(
        Input value,
        exec::GenericWriter& writer) {
      SIMDJSON_ASSIGN_OR_RAISE(auto type, value.type());

      if (isJsonType(writer.type())) {
        std::string_view json;
        SIMDJSON_ASSIGN_OR_RAISE(json, rawJson(value, type));
        auto& vectorWriter = writer.castTo<Varchar>();

        // The needNormalizeForJsonParse() just checks for escape sequences
        // in the string
        if (needNormalizeForJsonParse(json.data(), json.size())) {
          // Unescape the json string as calling raw_json does not unescape in
          // simdjson.
          auto size = unescapeSizeForJsonCast(json.data(), json.size());
          vectorWriter.resize(size);
          unescapeForJsonCast(json.data(), json.size(), vectorWriter.data());
        } else {
          vectorWriter.append(json);
        }
      } else {
        std::string_view s;
        switch (type) {
          case simdjson::ondemand::json_type::string: {
            SIMDJSON_ASSIGN_OR_RAISE(s, value.get_string(true));
            break;
          }
          case simdjson::ondemand::json_type::number:
          case simdjson::ondemand::json_type::boolean:
            s = value.raw_json_token();
            break;
          default:
            return simdjson::INCORRECT_TYPE;
        }
        writer.castTo<Varchar>().append(s);
      }

      return simdjson::SUCCESS;
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::BOOLEAN, Dummy> {
    static simdjson::error_code apply(
        Input value,
        exec::GenericWriter& writer) {
      SIMDJSON_ASSIGN_OR_RAISE(auto type, value.type());
      auto& w = writer.castTo<bool>();
      switch (type) {
        case simdjson::ondemand::json_type::boolean: {
          SIMDJSON_ASSIGN_OR_RAISE(w, value.get_bool());
          break;
        }
        case simdjson::ondemand::json_type::number: {
          SIMDJSON_ASSIGN_OR_RAISE(auto num, value.get_number());
          switch (num.get_number_type()) {
            case simdjson::ondemand::number_type::floating_point_number:
              w = num.get_double() != 0;
              break;
            case simdjson::ondemand::number_type::signed_integer:
              w = num.get_int64() != 0;
              break;
            case simdjson::ondemand::number_type::unsigned_integer:
              w = num.get_uint64() != 0;
              break;
            case simdjson::ondemand::number_type::big_integer:
              VELOX_UNREACHABLE(); // value.get_number() would have failed
                                   // already.
          }
          break;
        }
        case simdjson::ondemand::json_type::string: {
          SIMDJSON_ASSIGN_OR_RAISE(auto s, value.get_string());
          SIMDJSON_ASSIGN_OR_RAISE(w, fromString<bool>(s));
          break;
        }
        default:
          return simdjson::INCORRECT_TYPE;
      }
      return simdjson::SUCCESS;
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::TINYINT, Dummy> {
    static simdjson::error_code apply(
        Input value,
        exec::GenericWriter& writer) {
      return castJsonToInt<int8_t>(value, writer);
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::SMALLINT, Dummy> {
    static simdjson::error_code apply(
        Input value,
        exec::GenericWriter& writer) {
      return castJsonToInt<int16_t>(value, writer);
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::INTEGER, Dummy> {
    static simdjson::error_code apply(
        Input value,
        exec::GenericWriter& writer) {
      return castJsonToInt<int32_t>(value, writer);
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::BIGINT, Dummy> {
    static simdjson::error_code apply(
        Input value,
        exec::GenericWriter& writer) {
      return castJsonToInt<int64_t>(value, writer);
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::REAL, Dummy> {
    static simdjson::error_code apply(
        Input value,
        exec::GenericWriter& writer) {
      return castJsonToFloatingPoint<float>(value, writer);
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::DOUBLE, Dummy> {
    static simdjson::error_code apply(
        Input value,
        exec::GenericWriter& writer) {
      return castJsonToFloatingPoint<double>(value, writer);
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::ARRAY, Dummy> {
    static simdjson::error_code apply(
        Input value,
        exec::GenericWriter& writer) {
      auto& writerTyped = writer.castTo<Array<Any>>();
      auto& elementType = writer.type()->childAt(0);
      SIMDJSON_ASSIGN_OR_RAISE(auto array, value.get_array());
      for (auto elementResult : array) {
        SIMDJSON_ASSIGN_OR_RAISE(auto element, elementResult);
        // If casting to array of JSON, nulls in array elements should become
        // the JSON text "null".
        if (!isJsonType(elementType) && element.is_null()) {
          writerTyped.add_null();
        } else {
          SIMDJSON_TRY(VELOX_DYNAMIC_TYPE_DISPATCH(
              CastFromJsonTypedImpl<simdjson::ondemand::value>::apply,
              elementType->kind(),
              element,
              writerTyped.add_item()));
        }
      }
      return simdjson::SUCCESS;
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::MAP, Dummy> {
    static simdjson::error_code apply(
        Input value,
        exec::GenericWriter& writer) {
      auto& writerTyped = writer.castTo<Map<Any, Any>>();
      auto& keyType = writer.type()->childAt(0);
      auto& valueType = writer.type()->childAt(1);
      SIMDJSON_ASSIGN_OR_RAISE(auto object, value.get_object());
      for (auto fieldResult : object) {
        SIMDJSON_ASSIGN_OR_RAISE(auto field, fieldResult);
        SIMDJSON_ASSIGN_OR_RAISE(auto key, field.unescaped_key(true));
        // If casting to map of JSON values, nulls in map values should become
        // the JSON text "null".
        if (!isJsonType(valueType) && field.value().is_null()) {
          SIMDJSON_TRY(VELOX_DYNAMIC_TYPE_DISPATCH(
              appendMapKey, keyType->kind(), key, writerTyped.add_null()));
        } else {
          auto writers = writerTyped.add_item();
          SIMDJSON_TRY(VELOX_DYNAMIC_TYPE_DISPATCH(
              appendMapKey, keyType->kind(), key, std::get<0>(writers)));
          SIMDJSON_TRY(VELOX_DYNAMIC_TYPE_DISPATCH(
              CastFromJsonTypedImpl<simdjson::ondemand::value>::apply,
              valueType->kind(),
              field.value(),
              std::get<1>(writers)));
        }
      }
      return simdjson::SUCCESS;
    }
  };

  static folly::F14FastMap<std::string, int32_t> makeFieldIndicesMap(
      const RowType& rowType,
      bool allFieldsAreAscii) {
    folly::F14FastMap<std::string, int32_t> fieldIndices;
    const auto size = rowType.size();
    for (auto i = 0; i < size; ++i) {
      std::string key = rowType.nameOf(i);
      if (allFieldsAreAscii) {
        folly::toLowerAscii(key);
      } else {
        boost::algorithm::to_lower(key);
      }

      fieldIndices[key] = i;
    }

    return fieldIndices;
  }

  template <typename Dummy>
  struct KindDispatcher<TypeKind::ROW, Dummy> {
    static simdjson::error_code apply(
        Input value,
        exec::GenericWriter& writer) {
      auto& rowType = writer.type()->asRow();
      auto& writerTyped = writer.castTo<DynamicRow>();
      SIMDJSON_ASSIGN_OR_RAISE(auto type, value.type());
      if (type == simdjson::ondemand::json_type::array) {
        SIMDJSON_ASSIGN_OR_RAISE(auto array, value.get_array());
        SIMDJSON_ASSIGN_OR_RAISE(auto arraySize, array.count_elements());
        if (arraySize != writer.type()->size()) {
          return simdjson::INCORRECT_TYPE;
        }
        column_index_t i = 0;
        for (auto elementResult : array) {
          SIMDJSON_ASSIGN_OR_RAISE(auto element, elementResult);
          if (element.is_null()) {
            writerTyped.set_null_at(i);
          } else {
            SIMDJSON_TRY(VELOX_DYNAMIC_TYPE_DISPATCH(
                CastFromJsonTypedImpl<simdjson::ondemand::value>::apply,
                rowType.childAt(i)->kind(),
                element,
                writerTyped.get_writer_at(i)));
          }
          ++i;
        }
      } else {
        SIMDJSON_ASSIGN_OR_RAISE(auto object, value.get_object());

        // TODO Populate this mapping once, not per-row.
        // Mapping from lower-case field names of the target RowType to their
        // indices.
        bool allFieldsAreAscii = true;
        const auto size = rowType.size();
        for (auto i = 0; i < size; ++i) {
          const auto& name = rowType.nameOf(i);
          allFieldsAreAscii &=
              functions::stringCore::isAscii(name.data(), name.size());
        }

        auto fieldIndices = makeFieldIndicesMap(rowType, allFieldsAreAscii);

        std::string key;
        for (auto fieldResult : object) {
          SIMDJSON_ASSIGN_OR_RAISE(auto field, fieldResult);
          if (!field.value().is_null()) {
            SIMDJSON_ASSIGN_OR_RAISE(key, field.unescaped_key(true));

            // boost::algorithm::to_lower is very slow. Use much faster
            // folly::toLowerAscii if possible.
            if (allFieldsAreAscii) {
              folly::toLowerAscii(key);
            } else {
              boost::algorithm::to_lower(key);
            }

            auto it = fieldIndices.find(key);
            if (it != fieldIndices.end()) {
              const auto index = it->second;

              VELOX_USER_CHECK_GE(index, 0, "Duplicate field: {}", key);
              it->second = -1;

              SIMDJSON_TRY(VELOX_DYNAMIC_TYPE_DISPATCH(
                  CastFromJsonTypedImpl<simdjson::ondemand::value>::apply,
                  rowType.childAt(index)->kind(),
                  field.value(),
                  writerTyped.get_writer_at(index)));
            }
          }
        }

        for (const auto& [key, index] : fieldIndices) {
          if (index >= 0) {
            writerTyped.set_null_at(index);
          }
        }
      }
      return simdjson::SUCCESS;
    }
  };

  static simdjson::simdjson_result<std::string_view> rawJson(
      Input value,
      simdjson::ondemand::json_type type) {
    switch (type) {
      case simdjson::ondemand::json_type::array: {
        SIMDJSON_ASSIGN_OR_RAISE(auto array, value.get_array());
        return array.raw_json();
      }
      case simdjson::ondemand::json_type::object: {
        SIMDJSON_ASSIGN_OR_RAISE(auto object, value.get_object());
        return object.raw_json();
      }
      default:
        return value.raw_json_token();
    }
  }

  template <typename T>
  static simdjson::error_code castJsonToInt(
      Input value,
      exec::GenericWriter& writer) {
    SIMDJSON_ASSIGN_OR_RAISE(auto type, value.type());
    switch (type) {
      case simdjson::ondemand::json_type::number: {
        SIMDJSON_ASSIGN_OR_RAISE(auto num, value.get_number());
        switch (num.get_number_type()) {
          case simdjson::ondemand::number_type::floating_point_number:
            return convertIfInRange<T>(num.get_double(), writer);
          case simdjson::ondemand::number_type::signed_integer:
            return convertIfInRange<T>(num.get_int64(), writer);
          case simdjson::ondemand::number_type::unsigned_integer:
            return simdjson::NUMBER_OUT_OF_RANGE;
          case simdjson::ondemand::number_type::big_integer:
            VELOX_UNREACHABLE(); // value.get_number() would have failed
                                 // already.
        }
        break;
      }
      case simdjson::ondemand::json_type::boolean: {
        SIMDJSON_ASSIGN_OR_RAISE(auto b, value.get_bool());
        writer.castTo<T>() = b;
        break;
      }
      case simdjson::ondemand::json_type::string: {
        SIMDJSON_ASSIGN_OR_RAISE(auto s, value.get_string());
        SIMDJSON_ASSIGN_OR_RAISE(writer.castTo<T>(), fromString<T>(s));
        break;
      }
      default:
        return simdjson::INCORRECT_TYPE;
    }
    return simdjson::SUCCESS;
  }

  template <typename T>
  static simdjson::error_code castJsonToFloatingPoint(
      Input value,
      exec::GenericWriter& writer) {
    SIMDJSON_ASSIGN_OR_RAISE(auto type, value.type());
    switch (type) {
      case simdjson::ondemand::json_type::number: {
        SIMDJSON_ASSIGN_OR_RAISE(auto num, value.get_double());
        return convertIfInRange<T>(num, writer);
      }
      case simdjson::ondemand::json_type::boolean: {
        SIMDJSON_ASSIGN_OR_RAISE(auto b, value.get_bool());
        writer.castTo<T>() = b;
        break;
      }
      case simdjson::ondemand::json_type::string: {
        SIMDJSON_ASSIGN_OR_RAISE(auto s, value.get_string());
        SIMDJSON_ASSIGN_OR_RAISE(writer.castTo<T>(), fromString<T>(s));
        break;
      }
      default:
        return simdjson::INCORRECT_TYPE;
    }
    return simdjson::SUCCESS;
  }
};

template <TypeKind kind>
simdjson::error_code castFromJsonOneRow(
    simdjson::padded_string_view input,
    exec::VectorWriter<Any>& writer) {
  SIMDJSON_ASSIGN_OR_RAISE(auto doc, simdjsonParse(input));
  if (doc.is_null()) {
    writer.commitNull();
  } else {
    SIMDJSON_TRY(
        CastFromJsonTypedImpl<simdjson::ondemand::document&>::apply<kind>(
            doc, writer.current()));
    writer.commit(true);
  }
  return simdjson::SUCCESS;
}

bool isSupportedBasicType(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::BIGINT:
    case TypeKind::INTEGER:
    case TypeKind::SMALLINT:
    case TypeKind::TINYINT:
    case TypeKind::DOUBLE:
    case TypeKind::REAL:
    case TypeKind::VARCHAR:
      return true;
    default:
      return false;
  }
}
} // namespace

bool JsonCastOperator::isSupportedFromType(const TypePtr& other) const {
  if (isSupportedBasicType(other)) {
    return true;
  }

  switch (other->kind()) {
    case TypeKind::UNKNOWN:
    case TypeKind::TIMESTAMP:
      return true;
    case TypeKind::ARRAY:
      return isSupportedFromType(other->childAt(0));
    case TypeKind::ROW:
      for (const auto& child : other->as<TypeKind::ROW>().children()) {
        if (!isSupportedFromType(child)) {
          return false;
        }
      }
      return true;
    case TypeKind::MAP:
      if (other->childAt(1)->isUnKnown()) {
        if (other->childAt(0)->isUnKnown()) {
          return true;
        }
        return isSupportedBasicType(other->childAt(0)) &&
            !isJsonType(other->childAt(0));
      }

      return (
          isSupportedBasicType(other->childAt(0)) &&
          isSupportedFromType(other->childAt(1)));
    default:
      return false;
  }
}

template <TypeKind kind>
void JsonCastOperator::castFromJson(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    BaseVector& result) const {
  // Result is guaranteed to be a flat writable vector.
  auto* flatResult = result.as<typename KindToFlatVector<kind>::type>();
  exec::VectorWriter<Any> writer;
  writer.init(*flatResult);
  // Input is guaranteed to be in flat or constant encodings when passed in.
  auto* inputVector = input.as<SimpleVector<StringView>>();
  size_t maxSize = 0;
  rows.applyToSelected([&](auto row) {
    if (inputVector->isNullAt(row)) {
      return;
    }
    auto& input = inputVector->valueAt(row);
    maxSize = std::max(maxSize, input.size());
  });
  paddedInput_.resize(maxSize + simdjson::SIMDJSON_PADDING);
  context.applyToSelectedNoThrow(
      rows,
      [&](auto row) INLINE_LAMBDA {
        writer.setOffset(row);
        if (inputVector->isNullAt(row)) {
          writer.commitNull();
          return;
        }
        auto& input = inputVector->valueAt(row);
        memcpy(paddedInput_.data(), input.data(), input.size());
        simdjson::padded_string_view paddedInput(
            paddedInput_.data(), input.size(), paddedInput_.size());
        if (auto error = castFromJsonOneRow<kind>(paddedInput, writer)) {
          context.setVeloxExceptionError(row, errors_[error]);
          writer.commitNull();
        }
      },
      [&](vector_size_t row) INLINE_LAMBDA { writer.commitNull(); });

  writer.finish();
}

bool JsonCastOperator::isSupportedToType(const TypePtr& other) const {
  if (other->isDate()) {
    return false;
  }

  if (isSupportedBasicType(other)) {
    return true;
  }

  switch (other->kind()) {
    case TypeKind::ARRAY:
      return isSupportedToType(other->childAt(0));
    case TypeKind::ROW:
      for (const auto& child : other->as<TypeKind::ROW>().children()) {
        if (!isSupportedToType(child)) {
          return false;
        }
      }
      return true;
    case TypeKind::MAP:
      return (
          isSupportedBasicType(other->childAt(0)) &&
          isSupportedToType(other->childAt(1)) &&
          !isJsonType(other->childAt(0)));
    default:
      return false;
  }
}

/// Converts an input vector of a supported type to Json type. The
/// implementation follows the structure below.
/// JsonOperator::castTo: type dispatch for castToJson
/// +- castToJson (simple types)
///    +- generateJsonTyped: appends actual data to string
/// +- castToJson (complex types, via SFINAE)
///    +- castToJsonFrom{Row, Map, Array}:
///         Generates data for child vectors in temporary vectors. Copies this
///         data and adds delimiters and separators.
///       +- castToJson (recursive)
void JsonCastOperator::castTo(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    const TypePtr& resultType,
    VectorPtr& result) const {
  castTo(input, context, rows, resultType, result, nullptr);
}

void JsonCastOperator::castTo(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    const TypePtr& resultType,
    VectorPtr& result,
    const std::shared_ptr<exec::CastHooks>& hooks) const {
  context.ensureWritable(rows, resultType, result);
  auto* flatResult = result->as<FlatVector<StringView>>();

  // Casting from VARBINARY and OPAQUE are not supported and should have been
  // rejected by isSupportedType() in the caller.
  VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      castToJson, input.typeKind(), input, context, rows, *flatResult, hooks);
}

/// Converts an input vector from Json type to the type of result vector.
void JsonCastOperator::castFrom(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    const TypePtr& resultType,
    VectorPtr& result) const {
  // Initialize errors here so that we get the proper exception context.
  folly::call_once(
      initializeErrors_, [this] { simdjsonErrorsToExceptions(errors_); });
  context.ensureWritable(rows, resultType, result);
  // Casting to unsupported types should have been rejected by isSupportedType()
  // in the caller.
  VELOX_DYNAMIC_TYPE_DISPATCH(
      castFromJson, result->typeKind(), input, context, rows, *result);
}
} // namespace facebook::velox
