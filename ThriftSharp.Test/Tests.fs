namespace ThriftSharp.Test

open System
open ThriftSharp
open Thrift.Protocol
open Thrift.Transport
open Xunit
open FsCheck

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
}

type Test3 = {
    A: string
    B: MyClass
}

type Test<'a> = {
    A: Option<int>
    B: Set<string>
    C: Option<MyEnum>
    D: Map<string, 'a>
}

type Foo = 
    | Bar of Test<Test2> list
    | Baz of Test2[]
    | Dub of double

type MyClassProxy = {
    ID: string
}

type Generators =
    // Override the double generaor so we don't get NaNs etc which don't compare equal
    static member MyDoubleGenerator() =
        Arb.Default.Float() |> Arb.filter (fun d -> not(Double.IsNaN(d) || Double.IsInfinity(d)))


module Test =
    open DynamicThrift
    let proxies = [|
        Proxy.Create((fun (myClass: MyClass) -> { ID = myClass.ID }), (fun proxy -> MyClass(proxy.ID)))
    |]

    let tryGetProxy t = proxies |> Array.tryFind (fun proxy -> proxy.RealType.IsAssignableFrom(t))

    let private serialize x = 
        use memBuffer = new TMemoryBuffer()
        let prot = TBinaryProtocol(memBuffer)
        let wrapper = TBaseWrapper(x, tryGetProxy) :> TBase
        wrapper.Write(prot)
        memBuffer.GetBuffer()

    let private deserialize<'t> (mem: byte[]) = 
        use memBuffer = new TMemoryBuffer(mem)
        let prot = TBinaryProtocol(memBuffer)
        let wrapper = TBaseWrapper<'t>(tryGetProxy)
        (wrapper :> TBase).Read(prot)
        wrapper.Value

    let roundtrips x = 
        let bytes = x |> serialize 
        let newX = bytes |> deserialize
        x = newX

    [<Property>]
    let ``Test2 round-trips`` (x:Test2) = roundtrips x

    [<Property>]
    let ``Test3 round-trips`` (x:string) =
        let t3 = { Test3.A = x; B = MyClass(x) }
        roundtrips t3

    [<Property(Arbitrary=[|typeof<Generators>|])>]
    let ``Union round-trips`` (x:Foo) = roundtrips x

