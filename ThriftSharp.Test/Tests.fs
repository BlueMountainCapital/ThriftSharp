namespace ThriftSharp.Test

open System
open ThriftSharp
open Thrift.Protocol
open Thrift.Transport
open Xunit
open FsCheck

type MyEnum = 
    | Fuzzy = 0
    | Furry = 12

type Test2 = {
    X: int16
    Y: int32
    Z: int64
    AA: Option<string> * bool
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

type Generators =
    // Override the double generaor so we don't get NaNs etc which don't compare equal
    static member MyDoubleGenerator() =
        Arb.Default.Float() |> Arb.filter (fun d -> not(Double.IsNaN(d) || Double.IsInfinity(d)))

module Test = 
    let private serialize x = 
        use memBuffer = new TMemoryBuffer()
        let prot = TBinaryProtocol(memBuffer)
        let wrapper = TBaseWrapper(x) :> TBase
        wrapper.Write(prot)
        memBuffer.GetBuffer()

    let private deserialize (mem: byte[]) = 
        use memBuffer = new TMemoryBuffer(mem)
        let prot = TBinaryProtocol(memBuffer)
        let wrapper = TBaseWrapper()
        (wrapper :> TBase).Read(prot)
        wrapper.Value

    let roundtrips x = 
        let bytes = x |> serialize 
        let newX = bytes |> deserialize
        x = newX

    [<Property>]
    let ``Test2 round-trips`` (x:Test2) = roundtrips x

    [<Property(Arbitrary=[|typeof<Generators>|])>]
    let ``Union round-trips`` (x:Foo) = roundtrips x

