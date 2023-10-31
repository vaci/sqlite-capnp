// Copyright (c) 2023 Vaci Koblizek.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "serialize.h"
#include <kj/debug.h>
#include <kj/string-tree.h>
#include <kj/vector.h>

namespace sqlcap {


static constexpr uint64_t SQLTYPE_ANNOTATION_ID = 0xab6671fbf244a8deull;

kj::Maybe<kj::StringPtr> annotatedSqlType(capnp::StructSchema::Field field) {
  kj::Maybe<kj::StringPtr> sqlType;
  for (auto anno: field.getProto().getAnnotations()) {
    switch (anno.getId()) {
    case SQLTYPE_ANNOTATION_ID:
      sqlType = anno.getValue().getText();
      break;
    default:
      break;
    }
  }
  return sqlType;
}

kj::Maybe<kj::StringPtr> sqlType(capnp::StructSchema::Field field) {
  KJ_IF_MAYBE(s, annotatedSqlType(field)) {
    return *s;
  }

  auto type = field.getType().which();
  using Type = decltype(type);
  switch (type) {
  case Type::BOOL:
    return "UNSIGNED TINYINT"_kj;
  case Type::UINT8:
    return "UNSIGNED TINYINT"_kj;
  case Type::UINT16:
    return "UNSIGNED SMALLINT"_kj;
  case Type::UINT32:
    return "UNSIGNED INTEGER"_kj;
  case Type::UINT64:
    return "UNSIGNED INTEGER"_kj;
  case Type::INT8:
    return "TINYINT"_kj;
  case Type::INT16:
    return "SMALLINT"_kj;
  case Type::INT32:
  case Type::INT64:
    return "INTEGER"_kj;
  case Type::FLOAT32:
  case Type::FLOAT64:
    return "REAL"_kj;
  case Type::TEXT:
    return "TEXT"_kj;
  case Type::DATA:
    return "BLOB"_kj;
  default:
    return nullptr;
  }
}

kj::Array<capnp::StructSchema::Field> fields(capnp::StructSchema schema) {
  kj::Vector<capnp::StructSchema::Field> fields;
  for (auto&& field: schema.getFields()) {
    auto type = field.getType().which();
    using Type = decltype(type);
    switch (type) {
    case Type::BOOL:

    case Type::UINT8:
    case Type::UINT16:
    case Type::UINT32:
    case Type::UINT64:
      
    case Type::INT8:
    case Type::INT16:
    case Type::INT32:
    case Type::INT64:

    case Type::FLOAT32:
    case Type::FLOAT64:

    case Type::TEXT:
    case Type::DATA:
      fields.add(field);
      break;

    default:
      break;
    }
  }
  return fields.releaseAsArray();
}

kj::String create(capnp::StructSchema schema) {

  auto txt = kj::strTree(
    "CREATE TABLE foo (",
    kj::StringTree(KJ_MAP(field, fields(schema)) {
	auto name = field.getProto().getName();
	auto type = KJ_REQUIRE_NONNULL(sqlType(field));
	return kj::strTree(name, ' ', type);
      }, ", "), ") "
  );
  return txt.flatten();
}


kj::String insert(capnp::StructSchema schema) {

  auto txt = kj::strTree(
    "INSERT INTO foo (",
    kj::StringTree(KJ_MAP(field, fields(schema)) {
	auto name = field.getProto().getName();
	return kj::strTree(name);
      }, ", "), ") ",
    "VALUES(",
    kj::StringTree(KJ_MAP(field, fields(schema)) {
	return kj::strTree("?");
      }, ", "), ")"
    
  );
  return txt.flatten();
}

}
