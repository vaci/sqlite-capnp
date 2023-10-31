// Copyright (c) 2023 Vaci Koblizek.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "serialize.h"
#include <kj/debug.h>
#include <kj/string-tree.h>
#include <kj/vector.h>

namespace sqlcap {

static constexpr uint64_t SQLTYPE_ANNOTATION_ID = 0xab6671fbf244a8deull;
static constexpr uint64_t PRIMARY_KEY_ANNOTATION_ID = 0xbf80fc3031df0b60ull;
static constexpr uint64_t TABLE_ANNOTATION_ID = 0xb337d975d55c655aull;
static constexpr uint64_t SCHEMA_ANNOTATION_ID = 0x89ea0152d4a3dae3ull;

kj::Maybe<capnp::schema::Value::Reader> getAnnotation(
  capnp::List<capnp::schema::Annotation>::Reader annotations, uint64_t id) {
  for (auto anno: annotations) {
    if (anno.getId() == id) {
      return anno.getValue();
    }
  }
  return nullptr;
}

template <typename F>
auto mapAnnotation(
  capnp::List<capnp::schema::Annotation>::Reader annotations,
  uint64_t id, F&& func = [](auto value) { return value; }) {
  return getAnnotation(annotations, id).map(kj::mv(func));
}

kj::Maybe<kj::StringPtr> getAnnotationText(capnp::List<capnp::schema::Annotation>::Reader annotations, uint64_t id) {
  return mapAnnotation(annotations, id, [](auto value) { return value.getText(); });
}
  
kj::Maybe<kj::StringPtr> schemaName(capnp::StructSchema schema) {
  auto proto = schema.getProto();
  return getAnnotationText(proto.getAnnotations(), SCHEMA_ANNOTATION_ID);
}

kj::StringPtr shortName(capnp::StructSchema schema) {
  auto proto = schema.getProto();
  return proto.getDisplayName().slice(proto.getDisplayNamePrefixLength());
}

kj::StringPtr tableName(capnp::StructSchema schema) {
  auto proto = schema.getProto();
  return KJ_UNWRAP_OR_RETURN(
    getAnnotationText(proto.getAnnotations(), TABLE_ANNOTATION_ID),
    shortName(schema)
  );
}

kj::StringTree fullName(capnp::StructSchema schema) {
  auto name = tableName(schema);
  KJ_IF_MAYBE(s, schemaName(schema)) {
    return kj::strTree("[", *s, "].", name);
  }
  else {
    return kj::strTree(name);
  }
}

bool isPrimaryKey(capnp::StructSchema::Field field) {
  auto proto = field.getProto();
  auto value = KJ_UNWRAP_OR_RETURN(getAnnotation(proto.getAnnotations(), PRIMARY_KEY_ANNOTATION_ID), false);
  return value.getBool();
}

kj::Maybe<kj::StringPtr> annotatedSqlType(capnp::StructSchema::Field field) {
  auto proto = field.getProto();
  return getAnnotationText(proto.getAnnotations(), SQLTYPE_ANNOTATION_ID);
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

kj::Array<capnp::StructSchema::Field> pkFields(capnp::StructSchema schema) {
  kj::Vector<capnp::StructSchema::Field> fields;
  for (auto&& field: schema.getFields()) {
    if (isPrimaryKey(field)) {
      fields.add(field);
    }
  }
  return fields.releaseAsArray();
}

kj::Array<capnp::StructSchema::Field> valueFields(capnp::StructSchema schema) {
  kj::Vector<capnp::StructSchema::Field> fields;
  for (auto&& field: schema.getFields()) {
    if (!isPrimaryKey(field)) {
      fields.add(field);
    }
  }
  return fields.releaseAsArray();
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
    "CREATE TABLE ", fullName(schema), " (",
    kj::StringTree(KJ_MAP(field, fields(schema)) {
	auto name = field.getProto().getName();
	auto type = KJ_REQUIRE_NONNULL(sqlType(field));
	auto pk = isPrimaryKey(field);
	return kj::strTree(name, ' ', type, (pk ? " PRIMARY_KEY" : ""));
      }, ", "), ") "
  );
  return txt.flatten();
}


kj::String insert(capnp::StructSchema schema) {

  auto txt = kj::strTree(
    "INSERT INTO ", fullName(schema), " (",
    kj::StringTree(KJ_MAP(field, fields(schema)) {
	auto name = field.getProto().getName();
	return kj::strTree(name);
      }, ", "), ") ",
    " VALUES(",
    kj::StringTree(KJ_MAP(field, fields(schema)) {
	return kj::strTree("?");
      }, ", "), ")"
    
  );
  return txt.flatten();
}

kj::String update(capnp::StructSchema schema) {

  auto txt = kj::strTree(
    "UPDATE ", fullName(schema), " SET ",
    kj::StringTree(KJ_MAP(field, valueFields(schema)) {
	auto name = field.getProto().getName();
	return kj::strTree(name, " = ?");
      }, ", "),
    " WHERE ",
    kj::StringTree(KJ_MAP(field, pkFields(schema)) {
	auto name = field.getProto().getName();
	return kj::strTree(name, " = ?");
      }, " AND ")
    
  );
  return txt.flatten();
}

kj::String delete_(capnp::StructSchema schema) {

  auto txt = kj::strTree(
    "DELETE FROM ", fullName(schema), " WHERE ",
    kj::StringTree(KJ_MAP(field, pkFields(schema)) {
	auto name = field.getProto().getName();
	return kj::strTree(name, " = ?");
      }, " AND ")
  );
  return txt.flatten();
}

}
