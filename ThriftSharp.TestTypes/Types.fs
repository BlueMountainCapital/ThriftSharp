namespace ThriftSharp.TestTypes

open ThriftSharp

type MyClass(id: string) = 
    member this.ID = id
    override x.Equals y = y :? MyClass && x.ID = (y :?> MyClass).ID
    override x.GetHashCode() = x.ID.GetHashCode()

type MyEnum = 
    | Fuzzy = 0
    | Furry = 12

type Test2 = {
    X: int16
    Y: int32
    Z: int64
    AA: Option<string> * bool
    Ab: Option<string> * bool
}

type Test3 = {
    A: string
    B: MyClass
}

type Test4 = {
    t1: Test2
    t2: Test2
}

type Test<'a> = {
    A: Option<int>
    B: Set<string>
    C: Option<MyEnum>
    D: Map<string, 'a>
}

type Foo = 
    | Bar of Test<Test2> list
    | Baz of Test<Test4>[]
    | Dub of double

type MyClassProxy = {
    ID: string
}

module Proxies = 
    let private proxies = [|
        Proxy.Create((fun (myClass: MyClass) -> { ID = myClass.ID }), (fun proxy -> MyClass(proxy.ID)))
    |]

    let tryGetProxy t = proxies |> Array.tryFind (fun proxy -> proxy.RealType.IsAssignableFrom(t))


