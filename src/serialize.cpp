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
static constexpr uint64_t COLUMN_NAME_ANNOTATION_ID = 0xa9bcdb16cc5bbc7full;
static constexpr uint64_t TABLE_ANNOTATION_ID = 0xb337d975d55c655aull;
static constexpr uint64_t SCHEMA_ANNOTATION_ID = 0x89ea0152d4a3dae3ull;
static constexpr uint64_t IGNORE_ANNOTATION_ID = 0xddc3b0b27d076cd1ull;

kj::Maybe<capnp::schema::Value::Reader> getAnnotation(
  capnp::List<capnp::schema::Annotation>::Reader annotations, uint64_t id) {
  for (auto anno: annotations) {
    if (anno.getId() == id) {
      return anno.getValue();
    }
  }
  return nullptr;
}

kj::Maybe<kj::StringPtr> schemaName(capnp::StructSchema schema) {
  auto proto = schema.getProto();
  return getAnnotation(proto.getAnnotations(), SCHEMA_ANNOTATION_ID).map(
    [](auto value) { return value.getText(); }
  );
}

bool ignoreField(capnp::StructSchema::Field field) {
  auto proto = field.getProto();
  auto type = field.getType();

  if (type.isList() || type.isInterface() || type.isAnyPointer()) {
    return true;
  }

  return getAnnotation(proto.getAnnotations(), IGNORE_ANNOTATION_ID).map(
    [](auto value) { return value.getBool(); }
  ).orDefault(false);
}

kj::StringPtr columnName(capnp::StructSchema::Field field) {
  auto proto = field.getProto();
  return getAnnotation(proto.getAnnotations(), COLUMN_NAME_ANNOTATION_ID).map(
    [](auto value) { return value.getText(); }
  ).orDefault(proto.getName());
}


kj::StringPtr shortName(capnp::StructSchema schema) {
  auto proto = schema.getProto();
  return proto.getDisplayName().slice(proto.getDisplayNamePrefixLength());
}

kj::StringPtr tableName(capnp::StructSchema schema) {
  auto proto = schema.getProto();
  return KJ_UNWRAP_OR_RETURN(
    getAnnotation(proto.getAnnotations(), TABLE_ANNOTATION_ID).map(
      [](auto value) { return value.getText(); }
    ),
    shortName(schema)
  );
}

auto paramIndex(capnp::StructSchema::Field field) {
  return field.getIndex()+1;
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
   return getAnnotation(proto.getAnnotations(), PRIMARY_KEY_ANNOTATION_ID).map(
    [](auto value) { return value.getBool(); }
  ).orDefault(false);
}

kj::Maybe<kj::StringPtr> annotatedSqlType(capnp::StructSchema::Field field) {
  auto proto = field.getProto();
  return getAnnotation(proto.getAnnotations(), SQLTYPE_ANNOTATION_ID).map(
    [](auto value) { return value.getText(); }
  );
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
  case Type::ENUM:
    return "UNSIGNED SMALLINT"_kj;
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
// TODO
//  case Type::LIST:
// return "BLOB"_kj;
  default:
    return nullptr;
  }
}

auto pkFields(capnp::StructSchema schema) {
  kj::Vector<capnp::StructSchema::Field> fields;
  for (auto&& field: schema.getFields()) {
    if (isPrimaryKey(field) && !ignoreField(field)) {
      fields.add(field);
    }
  }
  return fields.releaseAsArray();
}

auto valueFields(capnp::StructSchema schema) {
  kj::Vector<capnp::StructSchema::Field> fields;
  for (auto&& field: schema.getFields()) {
    if (!isPrimaryKey(field) && !ignoreField(field)) {
      fields.add(field);
    }
  }
  return fields.releaseAsArray();
}

kj::Maybe<capnp::StructSchema::Field> fieldByName(capnp::StructSchema schema, kj::StringPtr name) {
  for (auto&& field: schema.getFields()) {
    if (ignoreField(field)) {
      continue;
    }

    if (name == columnName(field)) {
      return field;
    }
  }
  return nullptr;
}

auto fields(capnp::StructSchema schema) {
  kj::Vector<capnp::StructSchema::Field> fields;
  for (auto&& field: schema.getFields()) {
    if (ignoreField(field)) {
      continue;
    }
    auto type = field.getType().which();
    using Type = decltype(type);
    switch (type) {
    case Type::BOOL:
    case Type::ENUM:

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

      // case Type::LIST:
      //  fields.add(field);

    default:
      break;
    }
  }
  return fields.releaseAsArray();
}

kj::String createStatement(capnp::StructSchema schema) {
  return kj::strTree(
    "CREATE TABLE ", fullName(schema), " (",
    kj::StringTree(KJ_MAP(field, fields(schema)) {
	auto name = columnName(field);
	auto type = KJ_REQUIRE_NONNULL(sqlType(field));
	auto pk = isPrimaryKey(field);
	return kj::strTree(name, ' ', type, (pk ? " PRIMARY_KEY" : ""));
      }, ", "), ") "
  ).flatten();
}


kj::String insertStatement(capnp::StructSchema schema) {
  return kj::strTree(
    "INSERT INTO ", fullName(schema), " (",
    kj::StringTree(KJ_MAP(field, fields(schema)) {
	auto name = columnName(field);
	return kj::strTree(name);
      }, ", "), ") ",
    " VALUES(",
    kj::StringTree(KJ_MAP(field, fields(schema)) {
	return kj::strTree("?", paramIndex(field));
      }, ", "), ")"
    
  ).flatten();
}

kj::String updateStatement(capnp::StructSchema schema) {
  return kj::strTree(
    "UPDATE ", fullName(schema), " SET ",
    kj::StringTree(KJ_MAP(field, valueFields(schema)) {
	auto name = columnName(field);
	return kj::strTree(name, " = ?", paramIndex(field));
      }, ", "),
    " WHERE ",
    kj::StringTree(KJ_MAP(field, pkFields(schema)) {
	auto name = columnName(field);
	return kj::strTree(name, " = ?", paramIndex(field));
      }, " AND ")
    
  ).flatten();
}

kj::String deleteStatement(capnp::StructSchema schema) {
  return kj::strTree(
    "DELETE FROM ", fullName(schema), " WHERE ",
    kj::StringTree(KJ_MAP(field, pkFields(schema)) {
	auto name = columnName(field);
	return kj::strTree(name, " = ?", paramIndex(field));
      }, " AND ")
  ).flatten();
}

kj::String selectStatement(capnp::StructSchema schema) {
  return kj::strTree(
    "SELECT ",
    kj::StringTree(KJ_MAP(field, valueFields(schema)) {
	auto name = columnName(field);
	return kj::strTree(name);
      }, ", "),
    " FROM ", fullName(schema), " WHERE ",
    kj::StringTree(KJ_MAP(field, pkFields(schema)) {
	auto name = columnName(field);
	return kj::strTree(name, " = ?", paramIndex(field));
      }, " AND ")
    
  ).flatten();
}

struct Adapter::Impl {
  
  Impl(sqlite3* db, capnp::StructSchema schema)
    : db_{db}
    , schema_{schema} {

    auto flags =  SQLITE_PREPARE_PERSISTENT;
    
    {
      auto txt = createStatement(schema);
      sqlite3_prepare_v3(db_, txt.cStr(), txt.size(), flags, &createStatement_, nullptr);
    }
    {
      auto txt = insertStatement(schema);
      sqlite3_prepare_v3(db_, txt.cStr(), txt.size(), flags, &insertStatement_, nullptr);
    }
    {
      auto txt = updateStatement(schema);
      sqlite3_prepare_v3(db_, txt.cStr(), txt.size(), flags, &updateStatement_, nullptr);
    }
    {
      auto txt = deleteStatement(schema);
      sqlite3_prepare_v3(db_, txt.cStr(), txt.size(), flags, &deleteStatement_, nullptr);
    }
    {
      auto txt = selectStatement(schema);
      sqlite3_prepare_v3(db_, txt.cStr(), txt.size(), flags, &selectStatement_, nullptr);
    }
  }

  ~Impl() {
    sqlite3_finalize(createStatement_);
    sqlite3_finalize(insertStatement_);
    sqlite3_finalize(updateStatement_);
    sqlite3_finalize(deleteStatement_);
    sqlite3_finalize(selectStatement_);
  }

private:
  kj::HashMap<capnp::StructSchema::Field, HandlerBase*> fieldHandlers_;
  kj::HashMap<capnp::Type, HandlerBase*> typeHandlers_;

  sqlite3* db_;
  capnp::StructSchema schema_;
  sqlite3_stmt* createStatement_;
  sqlite3_stmt* insertStatement_;
  sqlite3_stmt* updateStatement_;
  sqlite3_stmt* deleteStatement_;
  sqlite3_stmt* selectStatement_;

  friend class Adapter;
};


Adapter::Adapter(sqlite3* db, capnp::StructSchema schema)
  : impl_{kj::heap<Impl>(db, schema)} {
}

Adapter::~Adapter() {
}

void Adapter::insert(capnp::DynamicStruct::Reader input) {
  sqlite3_clear_bindings(impl_->insertStatement_);

  for (auto field: impl_->schema_.getFields()) {
    if (ignoreField(field)) {
      continue;
    }

    auto value = input.get(field);
    auto type = field.getType();
    auto name = field.getProto().getName();
    auto index = paramIndex(field); 
    KJ_LOG(INFO, "Encoding", index, value, name);
    encode(value, type, impl_->insertStatement_, index);
  }
  
 repeat:
  int step = sqlite3_step(impl_->insertStatement_);
  if (step == SQLITE_DONE) {
    return;
  }

  if (step == SQLITE_ROW) {
    return;
  }

  if (step == SQLITE_ERROR) {
    auto msg = sqlite3_errmsg(impl_->db_);
    throw KJ_EXCEPTION(FAILED, msg);
  }

  throw KJ_EXCEPTION(UNIMPLEMENTED);
}

void Adapter::update(capnp::DynamicStruct::Reader input) {
  for (auto field: impl_->schema_.getFields()) {
    if (ignoreField(field)) {
      continue;
    }

    auto value = input.get(field);
    auto type = field.getType();
    
    encode(value, type, impl_->updateStatement_, paramIndex(field));
  }
}

void Adapter::select(capnp::DynamicStruct::Builder builder) {
  auto orphanage = capnp::Orphanage::getForMessageContaining(builder);

  for (auto field: impl_->schema_.getFields()) {
    if (ignoreField(field)) {
      continue;
    }

    if (!isPrimaryKey(field)) {
      continue;
    }
    auto value = builder.get(field);
    auto type = field.getType();
    
    auto name = field.getProto().getName();
    auto index = paramIndex(field);
    encode(value.asReader(), type, impl_->selectStatement_, index);
  }

 repeat:
  int step = sqlite3_step(impl_->selectStatement_);
  if (step == SQLITE_DONE) {
    return;
  }

  if (step == SQLITE_ROW) {
    auto len = sqlite3_column_count(impl_->selectStatement_);
    for (auto ii = 0; ii < len; ++ii) {
      auto name = sqlite3_column_name(impl_->selectStatement_, ii);

      KJ_IF_MAYBE(field, fieldByName(impl_->schema_, name)) {
	auto type = field->getType();
	auto value = decode(type, impl_->selectStatement_, ii, orphanage);
	if (value.getType() != capnp::DynamicValue::VOID) {
	  builder.adopt(*field, kj::mv(value));
	}
      }
    }
    goto repeat;
  }
 
  if (step == SQLITE_ERROR) {
    auto msg = sqlite3_errmsg(impl_->db_);
    throw KJ_EXCEPTION(FAILED, msg);
  }

  throw KJ_EXCEPTION(UNIMPLEMENTED);
  
}

void Adapter::encode(capnp::DynamicValue::Reader input, capnp::Type type, sqlite3_stmt* stmt, int param) const {
  KJ_IF_MAYBE(handler, impl_->typeHandlers_.find(type)) {
    return (*handler)->encodeBase(*this, input, stmt, param);
  }

  auto which = type.which();
  using Type = decltype(which);
  
  switch (which) {
  case Type::BOOL:
    sqlite3_bind_int(stmt, param, input.as<bool>() ? 1 : 0);
    break;
  case Type::ENUM:
    sqlite3_bind_int(stmt, param, input.as<capnp::DynamicEnum>().getRaw());
    break;
  case Type::INT64:
    sqlite3_bind_int64(stmt, param, input.as<int64_t>());
    break;
  case Type::UINT64:
    sqlite3_bind_int64(stmt, param, input.as<uint64_t>());
    break;
  case Type::FLOAT32:
    sqlite3_bind_double(stmt, param, input.as<float>());
    break;
  case Type::FLOAT64:	
    sqlite3_bind_double(stmt, param, input.as<float>());
    break;
  case Type::TEXT: {
    auto txt = input.as<capnp::Text>();
    sqlite3_bind_text(stmt, param, txt.cStr(), txt.size(), SQLITE_TRANSIENT);
    break;
  }
  case Type::DATA: {
    auto data = input.as<capnp::Data>();
    sqlite3_bind_blob(stmt, param, data.begin(), data.size(), SQLITE_TRANSIENT);
    break;
  }
  }
}

capnp::Orphan<capnp::DynamicValue> Adapter::decode(capnp::Type type, sqlite3_stmt* stmt, int col, capnp::Orphanage orphanage) const {
  KJ_IF_MAYBE(handler, impl_->typeHandlers_.find(type)) {
    return (*handler)->decodeBase(*this, stmt, col, orphanage);
  }

  auto colType = sqlite3_column_type(stmt, col);
  if (colType == SQLITE_NULL) {
    return capnp::VOID;
  }

  auto which = type.which();
  using Type = decltype(which);
  
  switch (which) {
  case Type::BOOL:
    KJ_REQUIRE(colType == SQLITE_INTEGER);
    return sqlite3_column_int(stmt, col) ? true : false;
  case Type::ENUM:
    KJ_REQUIRE(colType == SQLITE_INTEGER);
    return static_cast<uint16_t>(sqlite3_column_int(stmt, col));
  case Type::INT64:
    KJ_REQUIRE(colType == SQLITE_INTEGER);
    return sqlite3_column_int64(stmt, col);
  case Type::UINT64:
    KJ_REQUIRE(colType == SQLITE_INTEGER);
    return static_cast<uint64_t>(sqlite3_column_int64(stmt, col));
  case Type::FLOAT32:
    KJ_REQUIRE(colType == SQLITE_FLOAT);
    return static_cast<float>(sqlite3_column_double(stmt, col));
  case Type::FLOAT64:
    KJ_REQUIRE(colType == SQLITE_FLOAT);
    return sqlite3_column_double(stmt, col);	
  case Type::TEXT: {
    KJ_REQUIRE(colType == SQLITE_TEXT);
    auto len = sqlite3_column_bytes(stmt, col);
    KJ_REQUIRE(len >= 0);
    auto txt = reinterpret_cast<const char*>(sqlite3_column_text(stmt, col));
    return orphanage.newOrphanCopy(capnp::Text::Reader{txt, static_cast<size_t>(len)});
  }
  case Type::DATA: {
    KJ_REQUIRE(colType == SQLITE_BLOB);
    auto len = sqlite3_column_bytes(stmt, col);
    KJ_REQUIRE(len >= 0);
    auto data = reinterpret_cast<const kj::byte*>(sqlite3_column_blob(stmt, col));
    return orphanage.newOrphanCopy(capnp::Data::Reader{data, static_cast<size_t>(len)});
  }
  }
  return capnp::VOID;
}

void Adapter::encodeField(capnp::StructSchema::Field field, capnp::DynamicValue::Reader input, sqlite3_stmt* stmt, int param) const {
  KJ_IF_MAYBE(handler, impl_->fieldHandlers_.find(field)) {
    return (*handler)->encodeBase(*this, input, stmt, param);
  }

  return encode(input, field.getType(), stmt, param);
}

void Adapter::addFieldHandlerImpl(
  capnp::StructSchema::Field field, capnp::Type type, HandlerBase& handler) {
  KJ_REQUIRE(type == field.getType(),
    "handler type did not match field type for addFieldHandler()");
  impl_->fieldHandlers_.upsert(field, &handler, [](HandlerBase*& existing, HandlerBase* replacement) {
    KJ_REQUIRE(existing == replacement, "field already has a different registered handler");
  });  
}


}
