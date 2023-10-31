// Copyright (c) 2023 Vaci Koblizek.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "serialize.h"
#include "test.capnp.h"
#include <kj/debug.h>
#include <kj/main.h>

#include <gtest/gtest.h>

using namespace sqlcap;

struct SqliteTest
  : testing::Test {

  SqliteTest() {}
  ~SqliteTest() noexcept {}
};

TEST_F(SqliteTest, CreateTable) {
  auto schema = capnp::Schema::from<TestAllTypes>();
  auto str = create(schema);
  KJ_LOG(INFO, str);
}

TEST_F(SqliteTest, Insert) {
  auto schema = capnp::Schema::from<TestAllTypes>();
  auto str = insert(schema);
  KJ_LOG(INFO, str);
}

TEST_F(SqliteTest, Update) {
  auto schema = capnp::Schema::from<TestAllTypes>();
  auto str = update(schema);
  KJ_LOG(INFO, str);
}

TEST_F(SqliteTest, Delete) {
  auto schema = capnp::Schema::from<TestAllTypes>();
  auto str = delete_(schema);
  KJ_LOG(INFO, str);
}

int main(int argc, char* argv[]) {
  kj::TopLevelProcessContext processCtx{argv[0]};
  processCtx.increaseLoggingVerbosity();

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


