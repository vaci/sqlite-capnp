@0xeb275e9862b5d6d0;

using Cxx = import "/c++.capnp";
using Sql = import "sqlite.capnp";

enum TestEnum {
  foo @0;
  bar @1;
  baz @2;
  qux @3;
  quux @4;
  corge @5;
  grault @6;
  garply @7;
}

struct TestAllTypes  $Sql.table("foo") {
  voidField      @0  : Void $Sql.ignore(true);
  boolField      @1  : Bool;
  int8Field      @2  : Int8;
  int16Field     @3  : Int16;
  int32Field     @4  : Int32;
  int64Field     @5  : Int64;
  uInt8Field     @6  : UInt8;
  uInt16Field    @7  : UInt16;
  uInt32Field    @8  : UInt32;
  uInt64Field    @9  : UInt64;
  float32Field   @10 : Float32;
  float64Field   @11 : Float64;
  textField      @12 : Text;
  dataField      @13 : Data;
  structField    @14 : TestAllTypes $Sql.ignore(true);
  enumField      @15 : TestEnum;
  interfaceField @16 : Void $Sql.ignore(true);  # TODO

  voidList      @17 : List(Void) $Sql.ignore(true);
  boolList      @18 : List(Bool) $Sql.ignore(true);
  int8List      @19 : List(Int8) $Sql.ignore(true);
  int16List     @20 : List(Int16) $Sql.ignore(true);
  int32List     @21 : List(Int32) $Sql.ignore(true);
  int64List     @22 : List(Int64) $Sql.ignore(true);
  uInt8List     @23 : List(UInt8) $Sql.ignore(true);
  uInt16List    @24 : List(UInt16) $Sql.ignore(true);
  uInt32List    @25 : List(UInt32) $Sql.ignore(true);
  uInt64List    @26 : List(UInt64) $Sql.ignore(true);
  float32List   @27 : List(Float32) $Sql.ignore(true);
  float64List   @28 : List(Float64) $Sql.ignore(true);
  textList      @29 : List(Text) $Sql.ignore(true);
  dataList      @30 : List(Data) $Sql.ignore(true);
  structList    @31 : List(TestAllTypes) $Sql.ignore(true);
  enumList      @32 : List(TestEnum) $Sql.ignore(true);
  interfaceList @33 : List(Void) $Sql.ignore(true);  # TODO

  pkInt @34 : Int64 $Sql.primaryKey(true);
  pkText @35 : Text $Sql.primaryKey(true);
  ignoreMePk @36 : Text  $Sql.primaryKey(true) $Sql.ignore(true);
  ignoreMeValue @37 : Text  $Sql.ignore(true);
  
}

