/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include <optional>

#include <common/base/BoltException.h>
#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "bolt/functions/sparksql/ICURegexFunctions.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
namespace bytedance::bolt::functions::sparksql {
using namespace bytedance::bolt::test;
namespace {

class ICURegexFunctionsTest : public test::SparkFunctionBaseTest {
 public:
  template <typename T>
  void testRegexpExtractAll(
      const std::vector<std::optional<std::string>>& inputs,
      const std::vector<std::optional<std::string>>& patterns,
      const std::vector<std::optional<T>>& groupIds,
      const std::vector<std::optional<std::vector<std::string>>>& output);

  std::string generateString(
      const char* characterSet,
      vector_size_t outputLength = 60) {
    vector_size_t arrLen = strlen(characterSet);
    std::string output;

    for (int i = 0; i < outputLength; i++) {
      output += characterSet[i % arrLen];
    }
    return output;
  }
};

template <typename Table, typename Row, std::size_t... I>
void addRow(Table& dataset, const Row& row, std::index_sequence<I...>) {
  (std::get<I>(dataset).push_back(std::get<I>(row)), ...);
}

template <typename ReturnType, typename... T>
class VectorFunctionTester : public test::FunctionBaseTest {
 public:
  explicit VectorFunctionTester(std::string expr) : expr(expr) {}

  // Use to assert individual test cases.
  std::optional<ReturnType> operator()(const std::optional<T>&... args) {
    auto result = evaluateOnce<ReturnType>(expr, args...);
    addRow(this->args, std::tuple(args...), std::index_sequence_for<T...>());
    expected.push_back(result);
    return result;
  }

  // Generate, run, and verify function test cases tested earlier still pass
  // when in batches.
  void testBatchAll() {
    // Test a batch with all test cases in order.
    std::apply([&](auto... args) { testBatch(expected, args...); }, args);
    // Generate one batch per test case, with 1024 identical rows.
    for (int i = 0; i < expected.size(); ++i) {
      std::apply(
          [&](auto... args) {
            testBatch(
                std::vector(1024, expected[i]), std::vector(1024, args[i])...);
            testConstantBatch(expected[i], args[i]...);
          },
          args);
    }
  }

 private:
  using EvalReturnType = EvalType<ReturnType>;
  const std::string expr;
  std::tuple<std::vector<std::optional<T>>...> args;
  std::vector<std::optional<ReturnType>> expected;

  void testBatch(
      const std::vector<std::optional<ReturnType>>& expected,
      const std::vector<std::optional<T>>&... args) {
    auto actual = evaluate<SimpleVector<EvalReturnType>>(
        expr, makeRowVector({makeNullableFlatVector(args)...}));
    ASSERT_EQ(actual->size(), expected.size());
    for (int i = 0; i < expected.size(); ++i) {
      EXPECT_EQ(
          actual->isNullAt(i) ? std::optional<ReturnType>{}
                              : ReturnType(actual->valueAt(i)),
          expected[i]);
    }
  }

  void testConstantBatch(
      const std::optional<ReturnType>& expected,
      std::optional<T>&... args) {
    constexpr std::size_t blocksize = 1024;
    auto actual = evaluate<SimpleVector<EvalReturnType>>(
        expr, makeRowVector({makeConstant(args, blocksize)...}));
    ASSERT_EQ(actual->size(), blocksize);
    for (int i = 0; i < blocksize; ++i) {
      EXPECT_EQ(
          actual->isNullAt(i) ? std::optional<ReturnType>{}
                              : ReturnType(actual->valueAt(i)),
          expected);
    }
  }

  // We inherit from FunctionBaseTest so that we can get access to the helpers
  // it defines, but since it is supposed to be a test fixture TestBody() is
  // declared pure virtual.  We must provide an implementation here.
  void TestBody() override {}
};

template <typename F>
void testRegexpLikeDangling(F&& regexpLike, bool matchDanglingRightBrackets) {
  if (matchDanglingRightBrackets) {
    EXPECT_EQ(regexpLike("{][}", "}"), true);
    EXPECT_EQ(regexpLike("{][},", "},"), true);
    EXPECT_EQ(regexpLike(":},", "[[:space:]]},"), true);
    EXPECT_EQ(regexpLike("{][}", "]}"), false);
    EXPECT_EQ(
        regexpLike("zhongtong1cost\":2}", "zhongtong(.*?)cost\":(.*?)}"), true);
    EXPECT_THROW(regexpLike("{][}", "{][}"), BoltUserError);
    EXPECT_THROW(regexpLike("{][}", "\"},\{\""), BoltUserError);
  } else {
    EXPECT_EQ(regexpLike("{][}", "\\}"), true);
    EXPECT_THROW(regexpLike("{][}", "}"), BoltUserError);
    EXPECT_THROW(regexpLike(":},", "[[:space:]]},"), BoltUserError);
    EXPECT_THROW(regexpLike("{][},", "},"), BoltUserError);
    EXPECT_THROW(regexpLike("{][}", "]}"), BoltUserError);
    EXPECT_THROW(
        regexpLike("zhongtong1cost\":2}", "zhongtong(.*?)cost\":(.*?)}"),
        BoltUserError);
    EXPECT_THROW(regexpLike("{][}", "{][}"), BoltUserError);
    EXPECT_THROW(regexpLike("{][}", "\"},\{\""), BoltUserError);
  }
}

template <typename F>
void testRegexpLike(F&& regexpLike) {
  EXPECT_EQ(regexpLike(std::nullopt, "abdef"), std::nullopt);
  EXPECT_EQ(regexpLike("abdef", std::nullopt), std::nullopt);
  EXPECT_EQ(regexpLike(std::nullopt, std::nullopt), std::nullopt);

  // unix character class
  EXPECT_EQ(regexpLike(":", "[0-9[:space:]]"), true);
  EXPECT_EQ(regexpLike(":", "[^0-9[:space:]]"), false);
  EXPECT_EQ(regexpLike(":", "[[:space:]]"), true);
  EXPECT_EQ(regexpLike(":", "[^[:space:]]"), false);
  EXPECT_EQ(regexpLike("bounds", "[[:space:]]"), true);
  EXPECT_EQ(regexpLike("bounds", "[[:space:][:space:]]"), true);
  EXPECT_EQ(regexpLike("bound:", "[[:space:]]"), true);
  EXPECT_EQ(regexpLike(" bound", "[[:space:]]"), false);
  EXPECT_EQ(regexpLike(" bound]", "[[:space:]]"), false);
  EXPECT_EQ(regexpLike(",", "[a-z[:punct:]]"), false);
  EXPECT_EQ(regexpLike("pun", "[0-9[:punct:]]"), true);

  EXPECT_EQ(regexpLike("abdef", "abdef"), true);
  EXPECT_EQ(regexpLike("abbbbc", "a.*c"), true);
  EXPECT_EQ(regexpLike("fofo", "^fo"), true);
  EXPECT_EQ(regexpLike("afofo", "fo"), true);
  EXPECT_EQ(regexpLike("fo\no", "^fo\no"), true);
  EXPECT_EQ(regexpLike("Bn", "^Ba*n"), true);
  EXPECT_EQ(regexpLike("afofo", "fo"), true);
  EXPECT_EQ(regexpLike("afofo", "^fo"), false);
  EXPECT_EQ(regexpLike("Baan", "^Ba?n"), false);
  EXPECT_EQ(regexpLike("axe", "pi|apa"), false);
  EXPECT_EQ(regexpLike("pip", "^(pi)*$"), false);
  EXPECT_EQ(regexpLike("pip", "pi"), true);
  EXPECT_EQ(regexpLike("abc", "^ab"), true);
  EXPECT_EQ(regexpLike("abc", "^bc"), false);
  EXPECT_EQ(regexpLike("abc", "ab"), true);
  EXPECT_EQ(regexpLike("abc", "bc"), true);

  EXPECT_THROW(regexpLike("abbbbc", "**"), BoltUserError);
}

TEST_F(ICURegexFunctionsTest, regexpLikeConstantPattern) {
  auto f = [&](std::optional<std::string> str,
               std::optional<std::string> pattern) {
    return pattern
        ? evaluateOnce<bool>("icu_rlike(c0, '" + *pattern + "')", str, pattern)
        : std::nullopt;
  };
  testRegexpLike(f);

  {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kRegexMatchDanglingRightBrackets, "false"},
    });
    testRegexpLikeDangling(f, false);
  }

  {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kRegexMatchDanglingRightBrackets, "true"},
    });
    testRegexpLikeDangling(f, true);
  }
}

TEST_F(ICURegexFunctionsTest, regexpLike) {
  auto f = [&](std::optional<std::string> str,
               std::optional<std::string> pattern) {
    return evaluateOnce<bool>("icu_rlike(c0, c1)", str, pattern);
  };
  testRegexpLike(f);

  {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kRegexMatchDanglingRightBrackets, "false"},
    });
    testRegexpLikeDangling(f, false);
  }

  {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kRegexMatchDanglingRightBrackets, "true"},
    });
    testRegexpLikeDangling(f, true);
  }
}

template <typename F>
void testRegexpExtractDangling(
    F&& regexpExtract,
    bool matchDanglingRightBrackets) {
  if (matchDanglingRightBrackets) {
    EXPECT_EQ(regexpExtract("{][}", ".(]).(})", 1), "]");
    EXPECT_EQ(regexpExtract("{][}", ".(]).(})", 2), "}");
    EXPECT_THROW(regexpExtract("{][}", "{][}", 1), BoltUserError);
    EXPECT_EQ(
        regexpExtract("zhongtong1cost\":1}", "zhongtong(.*?)cost\":(.*?)}", 1),
        "1");
    EXPECT_EQ(
        regexpExtract("zhongtong1cost\":2}", "zhongtong(.*?)cost\":(.*?)}", 2),
        "2");
  } else {
    EXPECT_THROW(regexpExtract("{][}", ".(]).(})", 1), BoltUserError);
    EXPECT_THROW(regexpExtract("{][}", ".(]).(})", 2), BoltUserError);
    EXPECT_THROW(regexpExtract("{][}", "{][}", 1), BoltUserError);
    EXPECT_THROW(
        regexpExtract("zhongtong1cost\":1}", "zhongtong(.*?)cost\":(.*?)}", 1),
        BoltUserError);
    EXPECT_THROW(
        regexpExtract("zhongtong1cost\":2}", "zhongtong(.*?)cost\":(.*?)}", 2),
        BoltUserError);
  }
}

template <typename F>
void testRegexpExtract(F&& regexpExtract) {
  EXPECT_EQ(regexpExtract("100-200", "(\\d+)-(\\d+)", 1), "100");
  EXPECT_EQ(regexpExtract("100-200", "(\\d+)-(\\d+)", 2), "200");
  EXPECT_EQ(regexpExtract("100-200", "(\\d+).*", 1), "100");
  EXPECT_EQ(regexpExtract("100-200", "([a-z])", 1), "");
  EXPECT_EQ(regexpExtract("一龥三-一龥三", "(.*)-(.*)", 1), "一龥三");
  EXPECT_EQ(regexpExtract("\u8040-\u3060", "(.*)-(.*)", 1), "\u8040");
  EXPECT_EQ(regexpExtract("<👉123>\"", "(<.*>)", 1), "<👉123>");
  EXPECT_EQ(regexpExtract(std::nullopt, "([a-z])", 1), std::nullopt);
  EXPECT_EQ(regexpExtract("100-200", std::nullopt, 1), std::nullopt);
  EXPECT_EQ(regexpExtract("100-200", "([a-z])", std::nullopt), std::nullopt);
  EXPECT_THROW(regexpExtract("100-200", "(\\d+)-(\\d+)", 3), BoltUserError);
  EXPECT_THROW(regexpExtract("100-200", "(\\d+)-(\\d+)", -1), BoltUserError);
  EXPECT_THROW(regexpExtract("100-200", "(\\d+).*", 2), BoltUserError);
  EXPECT_THROW(regexpExtract("100-200", "\\d+", 1), BoltUserError);
  EXPECT_THROW(regexpExtract("100-200", "\\d+", -1), BoltUserError);
  EXPECT_THROW(regexpExtract("\"quote", "\"quote", 1), BoltUserError);
}

TEST_F(ICURegexFunctionsTest, regexExtract) {
  auto f = [&](std::optional<std::string> str,
               std::optional<std::string> pattern,
               std::optional<int> group) {
    return evaluateOnce<std::string>(
        "icu_regexp_extract(c0, c1, c2)", str, pattern, group);
  };

  testRegexpExtract(f);

  {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kRegexMatchDanglingRightBrackets, "false"},
    });
    testRegexpExtractDangling(f, false);
  }

  {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kRegexMatchDanglingRightBrackets, "true"},
    });
    testRegexpExtractDangling(f, true);
  }
  {
    std::string input = R"TEXT(<input>继续 <output>)TEXT";
    std::string pattern = "<input>([\\S\\s]*)<output>";
    EXPECT_EQ(f(input, pattern, 1).value_or("null"), "继续 ");
  }
}

TEST_F(ICURegexFunctionsTest, regexExtractBigintGroupId) {
  auto f = [&](std::optional<std::string> str,
               std::optional<std::string> pattern,
               std::optional<int64_t> group) {
    return evaluateOnce<std::string>(
        "icu_regexp_extract(c0, c1, c2)", str, pattern, group);
  };

  testRegexpExtract(f);

  {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kRegexMatchDanglingRightBrackets, "false"},
    });
    testRegexpExtractDangling(f, false);
  }

  {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kRegexMatchDanglingRightBrackets, "true"},
    });
    testRegexpExtractDangling(f, true);
  }
}

TEST_F(ICURegexFunctionsTest, regexExtractBatch) {
  VectorFunctionTester<std::string, std::string, std::string, int> re2Extract{
      "icu_regexp_extract(c0, c1, c2)"};
  testRegexpExtract(re2Extract);
  re2Extract.testBatchAll();
}

TEST_F(ICURegexFunctionsTest, regexExtractConstantPattern) {
  auto f = [&](std::optional<std::string> str,
               std::optional<std::string> pattern,
               std::optional<int> group) {
    return pattern
        ? evaluateOnce<std::string>(
              "icu_regexp_extract(c0, '" + *pattern + "', c1)", str, group)
        : std::nullopt;
  };

  testRegexpExtract(f);

  {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kRegexMatchDanglingRightBrackets, "false"},
    });
    testRegexpExtractDangling(f, false);
  }

  {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kRegexMatchDanglingRightBrackets, "true"},
    });
    testRegexpExtractDangling(f, true);
  }
}

TEST_F(ICURegexFunctionsTest, regexExtractNoGroupId) {
  auto extract =
      ([&](std::optional<std::string> str, std::optional<std::string> pattern) {
        return evaluateOnce<std::string>(
            "icu_regexp_extract(c0, c1)", str, pattern);
      });

  EXPECT_EQ(extract("a1 b2 c3", "(\\d+)"), "1");
  EXPECT_EQ(extract("a b245 c3", "(\\d+)"), "245");
}

TEST_F(ICURegexFunctionsTest, regexExtractConstantPatternNoGroupId) {
  auto extract =
      ([&](std::optional<std::string> str, std::optional<std::string> pattern) {
        return evaluateOnce<std::string>(
            "icu_regexp_extract(c0, '" + *pattern + "')", str);
      });

  EXPECT_EQ(extract("a1 b2 c3", "(\\d+)"), "1");
  EXPECT_EQ(extract("a b245 c3", "(\\d+)"), "245");
}

template <typename T>
void ICURegexFunctionsTest::testRegexpExtractAll(
    const std::vector<std::optional<std::string>>& inputs,
    const std::vector<std::optional<std::string>>& patterns,
    const std::vector<std::optional<T>>& groupIds,
    const std::vector<std::optional<std::vector<std::string>>>& output) {
  std::string constantPattern = "";
  std::string constantGroupId = "";
  std::string expression = "";

  auto result = [&] {
    auto input = makeFlatVector<StringView>(
        inputs.size(),
        [&inputs](vector_size_t row) {
          return inputs[row] ? StringView(*inputs[row]) : StringView();
        },
        [&inputs](vector_size_t row) { return !inputs[row].has_value(); });

    auto pattern = makeFlatVector<StringView>(
        patterns.size(),
        [&patterns](vector_size_t row) {
          return patterns[row] ? StringView(*patterns[row]) : StringView();
        },
        [&patterns](vector_size_t row) { return !patterns[row].has_value(); });
    if (patterns.size() == 1) {
      // Constant pattern
      constantPattern = std::string(", '") + patterns[0].value() + "'";
    }

    auto groupId = makeFlatVector<T>(
        groupIds.size(),
        [&groupIds](vector_size_t row) {
          return groupIds[row] ? *groupIds[row] : 0;
        },
        [&groupIds](vector_size_t row) { return !groupIds[row].has_value(); });
    if (groupIds.size() == 1) {
      // constant groupId
      constantGroupId = std::string(", ") + std::to_string(groupIds[0].value());
    }

    if (!constantPattern.empty()) {
      if (groupIds.empty()) {
        // Case 1: constant pattern, no groupId
        // for example: expression = icu_regexp_extract_all(c0,
        // '(\\d+)([a-z]+)')
        expression =
            std::string("icu_regexp_extract_all(c0") + constantPattern + ")";
        return evaluate(expression, makeRowVector({input}));
      } else if (!constantGroupId.empty()) {
        // Case 2: constant pattern, constant groupId
        // for example: expression = icu_regexp_extract_all(c0,
        // '(\\d+)([a-z]+)', 1)
        expression = std::string("icu_regexp_extract_all(c0") +
            constantPattern + constantGroupId + ")";
        return evaluate(expression, makeRowVector({input}));
      } else {
        // Case 3: constant pattern, variable groupId
        // for example: expression = icu_regexp_extract_all(c0,
        // '(\\d+)([a-z]+)', c1)
        expression = std::string("icu_regexp_extract_all(c0") +
            constantPattern + ", c1)";
        return evaluate(expression, makeRowVector({input, groupId}));
      }
    }

    // Case 4: variable pattern, no groupId
    if (groupIds.empty()) {
      // for example: expression = icu_regexp_extract_all(c0, c1)
      expression = std::string("icu_regexp_extract_all(c0, c1)");
      return evaluate(expression, makeRowVector({input, pattern}));
    }

    // Case 5: variable pattern, constant groupId
    if (!constantGroupId.empty()) {
      // for example: expression = icu_regexp_extract_all(c0, c1, 0)
      expression =
          std::string("icu_regexp_extract_all(c0, c1") + constantGroupId + ")";
      return evaluate(expression, makeRowVector({input, pattern}));
    }

    // Case 6: variable pattern, variable groupId
    expression = std::string("icu_regexp_extract_all(c0, c1, c2)");
    return evaluate(expression, makeRowVector({input, pattern, groupId}));
  }();

  // Creating vectors for output string vectors
  auto sizeAtOutput = [&output](vector_size_t row) {
    return output[row] ? output[row]->size() : 0;
  };
  auto valueAtOutput = [&output](vector_size_t row, vector_size_t idx) {
    return output[row] ? StringView(output[row]->at(idx)) : StringView("");
  };
  auto nullAtOutput = [&output](vector_size_t row) {
    return !output[row].has_value();
  };
  auto expectedResult = makeArrayVector<StringView>(
      output.size(), sizeAtOutput, valueAtOutput, nullAtOutput);

  // Checking the results
  assertEqualVectors(expectedResult, result);
}

TEST_F(ICURegexFunctionsTest, regexExtractAll) {
  const std::vector<std::optional<std::string>> inputs = {
      "100-200,300-400,500-600",
      "100-200,300-400,500-600",
      "100-200,300-400,500-600",
      "100-200,300-400,500-600",
      "100-200,300-400,500-600",
      std::nullopt,
      "100-200,300-400,500-600",
      "100-200,300-400,500-600",
      "一龥三-一龥三,\u8040-\u3060,一龥三-一龥三,"};
  const std::vector<std::optional<std::string>> patterns = {
      "(\\d+)-(\\d+)",
      "(\\d+)-(\\d+)",
      "(\\d+)-(\\d+)",
      "(\\d+).*",
      "([a-z])",
      "([a-z])",
      std::nullopt,
      "([a-z])",
      "([^,]*),"};
  const std::vector<std::optional<int32_t>> intGroupIds = {
      0, 1, 2, 1, 1, 1, 1, std::nullopt, 1};
  const std::vector<std::optional<int64_t>> bigGroupIds = {
      0, 1, 2, 1, 1, 1, 1, std::nullopt, 1};
  const std::vector<std::optional<std::vector<std::string>>> expectedOutputs = {
      {{"100-200", "300-400", "500-600"}},
      {{"100", "300", "500"}},
      {{"200", "400", "600"}},
      {{"100"}},
      emptyArray,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      {{"一龥三-一龥三", "\u8040-\u3060", "一龥三-一龥三"}}};

  testRegexpExtractAll(inputs, patterns, intGroupIds, expectedOutputs);
  testRegexpExtractAll(inputs, patterns, bigGroupIds, expectedOutputs);
}

TEST_F(ICURegexFunctionsTest, regexExtractAllConstant) {
  const std::vector<std::optional<std::string>> inputs = {
      "100-200,300-400,500-600",
      "100-200,300-400,500-600",
      "100-200,300-400,500-600",
      "100-200,300-400,500-600",
      "100-200,300-400,500-600",
      std::nullopt,
      "100-200,300-400,500-600",
      "100-200,300-400,500-600"};
  const std::vector<std::optional<std::string>> consPatterns = {
      "(\\d+)-(\\d+)"};
  const std::vector<std::optional<std::string>> patterns = {
      "(\\d+)-(\\d+)",
      "(\\d+)-(\\d+)",
      "(\\d+)-(\\d+)",
      "(\\d+).*",
      "([a-z])",
      "([a-z])",
      std::nullopt,
      "([a-z])"};
  const std::vector<std::optional<int32_t>> intGroupIds = {
      0, 1, 2, 1, 1, 1, 1, std::nullopt};
  const std::vector<std::optional<int32_t>> intConsGroupIds = {1};
  const std::vector<std::optional<int64_t>> bigGroupIds = {
      0, 1, 2, 1, 1, 1, 1, std::nullopt};
  const std::vector<std::optional<int32_t>> bigConsGroupIds = {1};
  const std::vector<std::optional<std::vector<std::string>>> consPatternOutput =
      {{{"100-200", "300-400", "500-600"}},
       {{"100", "300", "500"}},
       {{"200", "400", "600"}},
       {{"100", "300", "500"}},
       {{"100", "300", "500"}},
       std::nullopt,
       {{"100", "300", "500"}},
       std::nullopt};
  const std::vector<std::optional<std::vector<std::string>>> consGroupOutput = {
      {{"100", "300", "500"}},
      {{"100", "300", "500"}},
      {{"100", "300", "500"}},
      {{"100"}},
      emptyArray,
      std::nullopt,
      std::nullopt,
      emptyArray};

  const std::vector<std::optional<std::vector<std::string>>> bothConsOutput = {
      {{"100", "300", "500"}},
      {{"100", "300", "500"}},
      {{"100", "300", "500"}},
      {{"100", "300", "500"}},
      {{"100", "300", "500"}},
      std::nullopt,
      {{"100", "300", "500"}},
      {{"100", "300", "500"}}};

  testRegexpExtractAll(inputs, consPatterns, intGroupIds, consPatternOutput);
  testRegexpExtractAll(inputs, consPatterns, bigGroupIds, consPatternOutput);

  testRegexpExtractAll(inputs, patterns, intConsGroupIds, consGroupOutput);
  testRegexpExtractAll(inputs, patterns, bigConsGroupIds, consGroupOutput);

  testRegexpExtractAll(inputs, consPatterns, bigConsGroupIds, bothConsOutput);
  testRegexpExtractAll(inputs, consPatterns, bigConsGroupIds, bothConsOutput);
}

TEST_F(ICURegexFunctionsTest, regexExtractAllNoGroup) {
  const std::vector<std::optional<std::string>> inputs = {
      "100-200,300-400,500-600",
      "100-200,300-400,500-600",
      "100-200,300-400,500-600"};
  const std::vector<std::optional<std::string>> patterns = {
      "(\\d+)-(\\d+)", "(\\d+)*", "([a-z])"};
  const std::vector<std::optional<int32_t>> intGroupIds = {};
  const std::vector<std::optional<int64_t>> bigGroupIds = {};
  const std::vector<std::optional<std::vector<std::string>>> expectedOutputs = {
      {{"100", "300", "500"}}, {{"100"}}, emptyArray};

  size_t size = inputs.size();
}

template <typename F>
void testRegexpExtractAllError(F&& regexpExtractAll) {
  EXPECT_THROW(
      regexpExtractAll("100-200,300-400,500-600", "(\\d+)-(\\d+)", 3),
      BoltException);
  EXPECT_THROW(
      regexpExtractAll("100-200,300-400,500-600", "(\\d+).*", 2),
      BoltException);
  EXPECT_THROW(
      regexpExtractAll("100-200,300-400,500-600", "\\d+", 1), BoltException);
  EXPECT_THROW(
      regexpExtractAll("100-200,300-400,500-600", "(\\d+)-(\\d+)", -1),
      BoltException);
  EXPECT_THROW(
      regexpExtractAll("100-200,300-400,500-600", "\\d+", -1), BoltException);
}

TEST_F(ICURegexFunctionsTest, regexExtractAllBadArgs) {
  const auto noConstant = [&](std::optional<std::string> str,
                              std::optional<std::string> pattern,
                              std::optional<int32_t> groupId) {
    const std::string expression = "size(icu_regexp_extract_all(c0, c1, c2))";
    return evaluateOnce<int32_t>(expression, str, pattern, groupId);
  };
  testRegexpExtractAllError(noConstant);

  const auto constant = [&](std::optional<std::string> str,
                            std::optional<std::string> pattern,
                            std::optional<int32_t> groupId) {
    const std::string expression =
        fmt::format("size(icu_regexp_extract_all(c0, '{}', c1))", *pattern);
    return evaluateOnce<int32_t>(expression, str, groupId);
  };
}

template <typename F>
void testRegexpReplace(F&& regexpReplace) {
  EXPECT_EQ(regexpReplace("100-200", "(\\d+)", "num"), "num-num");
  EXPECT_EQ(regexpReplace("100-200", "(\\d+)", "###"), "###-###");
  EXPECT_EQ(regexpReplace("100-200", "(-)", "###"), "100###200");
  EXPECT_EQ(regexpReplace(std::nullopt, "(\\d+)", "###"), std::nullopt);
  EXPECT_EQ(regexpReplace("100-200", std::nullopt, "###"), std::nullopt);
  EXPECT_EQ(regexpReplace("100-200", "(-)", std::nullopt), std::nullopt);
  EXPECT_EQ(regexpReplace("一龥三", "(\\d+)\\.(\\d+)", "$1"), "一龥三");
  EXPECT_EQ(
      regexpReplace("\u8040-\u3060", "(\\d+)\\.(\\d+)", "$1"), "\u8040-\u3060");
  EXPECT_EQ(
      regexpReplace(
          "（台湾）林信良",
          "（[\\w\\W]*?）|\\([\\w\\W]*?\\)|【[\\w\\W]*?】|[^0-9a-zA-Z\u4e00-\u9fa5]",
          ""),
      "林信良");

  // test position
  EXPECT_EQ(regexpReplace("100-200", "(\\d+)", "num", 4), "100-num");
  EXPECT_EQ(regexpReplace("100-200", "(\\d+)", "###", 4), "100-###");
  EXPECT_EQ(regexpReplace("100-200", "(-)", "###", 4), "100###200");
  EXPECT_EQ(regexpReplace(std::nullopt, "(\\d+)", "###", 4), std::nullopt);
  EXPECT_EQ(regexpReplace("100-200", std::nullopt, "###", 4), std::nullopt);
  EXPECT_EQ(regexpReplace("100-200", "(-)", std::nullopt, 4), std::nullopt);
  EXPECT_EQ(regexpReplace("100-200", "(\\d+)", "num", 7), "100-20num");
  EXPECT_EQ(regexpReplace("100-200", "(\\d+)", "###", 7), "100-20###");
  EXPECT_EQ(regexpReplace("100-200", "(\\d+)", "num", 8), "100-200");
  EXPECT_EQ(regexpReplace("100-200", "(\\d+)", "###", 8), "100-200");
}

template <typename F>
void testRegexpCharacterClass(F&& regexpReplace) {
  // test character class \d
  EXPECT_EQ(
      regexpReplace("The numbers are 123 and \u0969\u0968", "(\\d+)", "num"),
      "The numbers are num and \u0969\u0968");
  EXPECT_EQ(
      regexpReplace(
          "The numbers are 123 and \u0969\u0968", "[^\\d]+", "non-num"),
      "non-num123non-num");

  // test character class \w
  EXPECT_EQ(
      regexpReplace("The numbers are 123_456 和 一二三", "(\\w+)", "char"),
      "char char char char 和 一二三");
  EXPECT_EQ(
      regexpReplace("are 123_456 和 一二三", "[^\\w]+", "?"), "are?123_456?");

  // test character class \s
  EXPECT_EQ(
      regexpReplace("\nare\t123\x0B and\u3000456\u2006", "(\\s+)", "?"),
      "?are?123?and\u3000456\u2006");
  EXPECT_EQ(
      regexpReplace("\nare\t123\x0B and\u3000456\u2006", "[^\\s]+", "?"),
      "\n?\t?\x0B ?");

  // mix multi char class and square bracket
  EXPECT_EQ(
      regexpReplace(
          "\nare\t123\x0B and\u3000456\u2006一二三", "[\\s\\w]+", "?"),
      "?\u3000?\u2006一二三");
  EXPECT_EQ(
      regexpReplace(
          "\nare\t123\x0B and\u3000456\u2006一二三", "[[\\s\\w]]+", "?"),
      "?\u3000?\u2006一二三");
  EXPECT_EQ(
      regexpReplace(
          "\nare\t123\x0B and\u3000456\u2006一二三", "[\\s[\\w]]+", "?"),
      "?\u3000?\u2006一二三");
  EXPECT_EQ(
      regexpReplace(
          "\nare\t123\x0B and\u3000456\u2006一二三", "[^\\s\\w]+", "?"),
      "\nare\t123\x0B and?456?");
  EXPECT_EQ(
      regexpReplace(
          "\nare\t123\x0B and\u3000456\u2006一二三", "[^[\\s][\\w]]+", "?"),
      "\nare\t123\x0B and?456?");
}

TEST_F(ICURegexFunctionsTest, regexpReplaceConstantPattern) {
  testRegexpReplace([&](std::optional<std::string> str,
                        std::optional<std::string> pattern,
                        std::optional<std::string> replacement,
                        std::optional<int32_t> pos = std::nullopt) {
    if (pos.has_value()) {
      return pattern
          ? evaluateOnce<std::string>(
                fmt::format(
                    "icu_regexp_replace(c0, '{}', c1, {})", *pattern, *pos),
                str,
                replacement)
          : std::nullopt;
    }
    return pattern ? evaluateOnce<std::string>(
                         "icu_regexp_replace(c0, '" + *pattern + "', c1)",
                         str,
                         replacement)
                   : std::nullopt;
  });
}

TEST_F(ICURegexFunctionsTest, regexpReplace) {
  testRegexpReplace([&](std::optional<std::string> str,
                        std::optional<std::string> pattern,
                        std::optional<std::string> replacement,
                        std::optional<int32_t> pos = std::nullopt) {
    if (pos.has_value()) {
      return evaluateOnce<std::string>(
          fmt::format("icu_regexp_replace(c0, c1, c2, {})", *pos),
          str,
          pattern,
          replacement);
    }
    return evaluateOnce<std::string>(
        "icu_regexp_replace(c0, c1, c2)", str, pattern, replacement);
  });
}

TEST_F(ICURegexFunctionsTest, regexpReplaceCharClass) {
  testRegexpCharacterClass([&](std::optional<std::string> str,
                               std::optional<std::string> pattern,
                               std::optional<std::string> replacement,
                               std::optional<int32_t> pos = std::nullopt) {
    if (pos.has_value()) {
      return pattern
          ? evaluateOnce<std::string>(
                fmt::format(
                    "icu_regexp_replace(c0, '{}', c1, {})", *pattern, *pos),
                str,
                replacement)
          : std::nullopt;
    }
    return pattern ? evaluateOnce<std::string>(
                         "icu_regexp_replace(c0, '" + *pattern + "', c1)",
                         str,
                         replacement)
                   : std::nullopt;
  });
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql
