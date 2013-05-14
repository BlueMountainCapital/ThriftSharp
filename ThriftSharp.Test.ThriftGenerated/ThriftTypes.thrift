namespace csharp ThriftSharp.Test.ThriftGenerated

struct MyClassProxy {
	1: required string ID
}

enum MyEnum {
	Fuzzy = 0
	Furry = 12
}

struct Tuple_optional_string_bool {
	1: optional string Item1
	2: required bool Item2
}

struct Test2 {
	1: required i16 X
	2: required i32 Y
	3: required i64 Z
	4: required Tuple_optional_string_bool AA
	5: required Tuple_optional_string_bool Ab
}

struct Test3 {
	1: required string A
	2: required MyClassProxy B
}

struct Test4 {
	1: required Test2 t1
	2: required Test2 t2
}

struct Test_Test2 {
	1: optional i32 A
	2: required set<string> B
	3: optional MyEnum C
	4: required map<string,Test2> D
}

struct Test_Test4 {
	1: optional i32 A
	2: required set<string> B
	3: optional MyEnum C
	4: required map<string,Test4> D
}

union Foo {
	1: list<Test_Test2> Bar
	2: list<Test_Test4> Baz
	3: double Dub
}