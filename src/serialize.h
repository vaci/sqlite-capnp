#pragma once
// Copyright (c) 2023 Vaci Koblizek.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include <sqlite3.h>
#include <capnp/common.h>
#include <capnp/dynamic.h>
#include <capnp/orphan.h>
#include <capnp/schema.h>
#include <kj/string.h>
#include <kj/map.h>

KJ_DECLARE_NON_POLYMORPHIC(sqlite3_value);

namespace sqlcap {

struct Adapter {
  explicit Adapter(sqlite3* db, capnp::StructSchema);

  ~Adapter();

  void insert(capnp::DynamicStruct::Reader);
  void update(capnp::DynamicStruct::Reader);
  void select(capnp::DynamicStruct::Builder);

  template <typename T, capnp::Style s = capnp::style<T>()>
  class Handler;
  
  template <typename T>
  void addFieldHandler(capnp::StructSchema::Field field, Handler<T>& handler);

  void encode(capnp::DynamicValue::Reader value, capnp::Type type, sqlite3_stmt*, int) const;
  
  void encodeField(capnp::StructSchema::Field field, capnp::DynamicValue::Reader input, sqlite3_stmt*, int) const;

  capnp::Orphan<capnp::DynamicValue> decode(capnp::Type type, sqlite3_stmt*, int, capnp::Orphanage) const;

private:
  struct HandlerBase;
  struct Impl;
  KJ_DECLARE_NON_POLYMORPHIC(Impl);
  
  void addFieldHandlerImpl(
    capnp::StructSchema::Field field, capnp::Type type, HandlerBase& handler);

  kj::Own<Impl> impl_;
};


struct Adapter::HandlerBase {
  virtual void encodeBase(const Adapter& codec, capnp::DynamicValue::Reader input, sqlite3_stmt* stmt, int param) const = 0;

  virtual capnp::Orphan<capnp::DynamicValue> decodeBase(const Adapter& codec, sqlite3_stmt* stmt, int param, capnp::Orphanage) const = 0;
};

template <typename T>
class Adapter::Handler<T, capnp::Style::POINTER>: private Adapter::HandlerBase {
public:
  virtual void encode(
    const Adapter& codec, capnp::ReaderFor<T> input, sqlite3_stmt* stmt, int param) const = 0;

  virtual capnp::Orphan<capnp::DynamicValue> decode(
    const Adapter& codec, capnp::Type, sqlite3_stmt* stmt, int col, capnp::Orphanage) const = 0;

private:
  void encodeBase(
    const Adapter& codec, capnp::DynamicValue::Reader input,
    sqlite3_stmt* stmt, int param) const override final {
    encode(codec, input.as<T>(), stmt, param);
  }

  capnp::Orphan<capnp::DynamicValue> decodeBase(
    const Adapter& codec, capnp::Type type,
    sqlite3_stmt* stmt, int col, capnp::Orphanage orphanage) const override final {
    return decode(codec, type, stmt, col, orphanage);
  }

  friend class Adapter;
};

template <typename T>
inline void Adapter::addFieldHandler(capnp::StructSchema::Field field, Handler<T>& handler) {
  addFieldHandlerImpl(field, capnp::Type::from<T>(), handler);
}

kj::String createStatement(capnp::StructSchema schema);
kj::String insertStatement(capnp::StructSchema schema);
kj::String updateStatement(capnp::StructSchema schema);
kj::String deleteStatement(capnp::StructSchema schema);
kj::String selectStatement(capnp::StructSchema schema);

kj::Own<Adapter> adapt(capnp::StructSchema);
}
