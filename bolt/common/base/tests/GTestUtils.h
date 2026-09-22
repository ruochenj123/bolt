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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include <gtest/gtest.h>
#include <cstdlib>

// gtest v1.10 deprecated *_TEST_CASE in favor of *_TEST_SUITE. These
// macros are provided for portability between different gtest versions.
#ifdef TYPED_TEST_SUITE
#define BOLT_TYPED_TEST_SUITE TYPED_TEST_SUITE
#else
#define BOLT_TYPED_TEST_SUITE TYPED_TEST_CASE
#endif

#ifdef INSTANTIATE_TEST_SUITE_P
#define BOLT_INSTANTIATE_TEST_SUITE_P INSTANTIATE_TEST_SUITE_P
#else
#define BOLT_INSTANTIATE_TEST_SUITE_P INSTANTIATE_TEST_CASE_P
#endif

#define BOLT_ASSERT_THROW_IMPL(_type, _expression, _errorMessage)     \
  try {                                                               \
    (_expression);                                                    \
    FAIL() << "Expected an exception";                                \
  } catch (const _type& e) {                                          \
    ASSERT_TRUE(e.message().find(_errorMessage) != std::string::npos) \
        << "Expected error message to contain '" << (_errorMessage)   \
        << "', but received '" << e.message() << "'.";                \
  }

#define BOLT_ASSERT_THROW(_expression, _errorMessage) \
  BOLT_ASSERT_THROW_IMPL(                             \
      bytedance::bolt::BoltException, _expression, _errorMessage)

#define BOLT_ASSERT_USER_THROW(_expression, _errorMessage) \
  BOLT_ASSERT_THROW_IMPL(                                  \
      bytedance::bolt::BoltUserError, _expression, _errorMessage)

#define BOLT_ASSERT_RUNTIME_THROW(_expression, _errorMessage) \
  BOLT_ASSERT_THROW_IMPL(                                     \
      bytedance::bolt::BoltRuntimeError, _expression, _errorMessage)

#define BOLT_ASSERT_ERROR_STATUS(_expression, _statusCode, _errorMessage) \
  const auto status = (_expression);                                      \
  ASSERT_TRUE(status.code() == _statusCode)                               \
      << "Expected error code to be '" << toString(_statusCode)           \
      << "', but received '" << toString(status.code()) << "'.";          \
  ASSERT_TRUE(status.message().find(_errorMessage) != std::string::npos)  \
      << "Expected error message to contain '" << (_errorMessage)         \
      << "', but received '" << status.message() << "'."

#define BOLT_ASSERT_ERROR_CODE_IMPL(_type, _expression, _errorCode)           \
  try {                                                                       \
    (_expression);                                                            \
    FAIL() << "Expected an exception";                                        \
  } catch (const _type& e) {                                                  \
    ASSERT_TRUE(e.errorCode() == _errorCode)                                  \
        << "Expected error code to be '" << _errorCode << "', but received '" \
        << e.errorCode() << "'.";                                             \
  }

#define BOLT_ASSERT_THROW_CODE(_expression, _errorCode) \
  BOLT_ASSERT_ERROR_CODE_IMPL(                          \
      bytedance::bolt::BoltException, _expression, _errorCode)

#define BOLT_ASSERT_USER_THROW_CODE(_expression, _errorCode) \
  BOLT_ASSERT_ERROR_CODE_IMPL(                               \
      bytedance::bolt::BoltUserError, _expression, _errorCode)
#define BOLT_ASSERT_RUNTIME_THROW_CODE(_expression, _errorCode) \
  BOLT_ASSERT_ERROR_CODE_IMPL(                                  \
      bytedance::bolt::BoltRuntimeError, _expression, _errorCode)

#ifndef NDEBUG
#define DEBUG_ONLY_TEST(test_fixture, test_name) TEST(test_fixture, test_name)
#define DEBUG_ONLY_TEST_F(test_fixture, test_name) \
  TEST_F(test_fixture, test_name)
#define DEBUG_ONLY_TEST_P(test_fixture, test_name) \
  TEST_P(test_fixture, test_name)
#else
#define DEBUG_ONLY_TEST(test_fixture, test_name) \
  TEST(test_fixture, DISABLED_##test_name)
#define DEBUG_ONLY_TEST_F(test_fixture, test_name) \
  TEST_F(test_fixture, DISABLED_##test_name)
#define DEBUG_ONLY_TEST_P(test_fixture, test_name) \
  TEST_P(test_fixture, DISABLED_##test_name)
#endif

namespace bytedance::bolt::test {
inline struct GTestEnvSetter {
  GTestEnvSetter() {
    ::setenv("BOLT_IN_GTEST", "1", 1);
  }
} g_gtest_env_setter;
} // namespace bytedance::bolt::test
