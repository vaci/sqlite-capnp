// Copyright (c) 2023 Vaci Koblizek.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "serialize.h"
#include "test.capnp.h"
#include <kj/debug.h>
#include <kj/exception.h>
#include <kj/main.h>

#include <sqlite3.h>

#include <gtest/gtest.h>

using namespace sqlcap;

struct SqliteTest
  : testing::Test {

  SqliteTest();
  ~SqliteTest() noexcept;

  void exec(kj::StringPtr);

  sqlite3* db_;
};

SqliteTest::SqliteTest() {
  int sqlite3_open_v2(
  const char *filename,   /* Database filename (UTF-8) */
  sqlite3 **ppDb,         /* OUT: SQLite db handle */
  int flags,              /* Flags */
  const char *zVfs        /* Name of VFS module to use */
);

  sqlite3_open_v2(":memory:", &db_, SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, nullptr);
}

int callback(void*, int len, char** values, char** columns) {
  for (int ii = 0; ii < len; ++ii) {
    auto column = columns[ii];
    auto value = values[ii];
    if (!value) value = strdup("NULL");
    KJ_LOG(INFO, ii, len, column, value);
  }
  return 0;
}

void SqliteTest::exec(kj::StringPtr txt) {

 char* errmsg;
 auto err = sqlite3_exec(db_, txt.cStr(), callback, nullptr, &errmsg);
 if (err == SQLITE_OK) {
   return;
 }

 KJ_LOG(ERROR, errmsg);
 throw KJ_EXCEPTION(FAILED, errmsg);
}

SqliteTest::~SqliteTest() noexcept {
  sqlite3_close(db_);
}

TEST_F(SqliteTest, CreateTable) {
  auto schema = capnp::Schema::from<TestAllTypes>();
  auto txt = createStatement(schema);
  KJ_LOG(INFO, txt);
  exec(txt);
}

TEST_F(SqliteTest, Insert) {
  auto schema = capnp::Schema::from<TestAllTypes>();
  exec(createStatement(schema));
  auto str = insertStatement(schema);
  KJ_LOG(INFO, str);

  capnp::MallocMessageBuilder mb;
  auto root = mb.initRoot<TestAllTypes>();
  root.setBoolField(true);
  root.setInt8Field(-4);
  root.setInt32Field(-4);
  root.setFloat32Field(543.21);
  root.setFloat64Field(543.21);

  root.setPkInt(23);
  root.setPkText("foo");
  
  Adapter adapter{db_, schema};
  adapter.insert(root.asReader());

  exec("SELECT * FROM foo");

  {
    auto key = mb.initRoot<TestAllTypes>();
    key.setPkInt(23);
    key.setPkText("foo");

    adapter.select(key);
  }
}

TEST_F(SqliteTest, Update) {
  auto schema = capnp::Schema::from<TestAllTypes>();
  auto str = updateStatement(schema);
  KJ_LOG(INFO, str);
}

TEST_F(SqliteTest, Delete) {
  auto schema = capnp::Schema::from<TestAllTypes>();
  auto str = deleteStatement(schema);
  KJ_LOG(INFO, str);
}

TEST_F(SqliteTest, Select) {
  auto schema = capnp::Schema::from<TestAllTypes>();
  auto txt = selectStatement(schema);
  KJ_LOG(INFO, txt);
}

int main(int argc, char* argv[]) {
  kj::TopLevelProcessContext processCtx{argv[0]};
  processCtx.increaseLoggingVerbosity();

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


