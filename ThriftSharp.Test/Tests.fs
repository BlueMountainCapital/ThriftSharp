namespace ThriftSharp.Test

open System
open ThriftSharp
open ThriftSharp.TestTypes
open Thrift.Protocol
open Thrift.Transport
open Xunit
open FsCheck

type Generators =
    // Override the double generaor so we don't get NaNs etc which don't compare equal
    static member MyDoubleGenerator() =
        Arb.Default.Float() |> Arb.filter (fun d -> not(Double.IsNaN(d) || Double.IsInfinity(d)))

module Test =
    let private serialize x = 
        use memBuffer = new TMemoryBuffer()
        let prot = TBinaryProtocol(memBuffer)
        let wrapper = TBaseWrapper(x, Proxies.tryGetProxy) :> TBase
        wrapper.Write(prot)
        memBuffer.GetBuffer()

    let private deserialize<'t> (mem: byte[]) = 
        use memBuffer = new TMemoryBuffer(mem)
        let prot = TBinaryProtocol(memBuffer)
        let wrapper = TBaseWrapper<'t>(Proxies.tryGetProxy)
        (wrapper :> TBase).Read(prot)
        wrapper.Value

    let roundtrips x = 
        let bytes = x |> serialize 
        let newX = bytes |> deserialize
        x = newX

    let roundtripsVia<'fsType, 'csType  when 'csType : (new : unit -> 'csType) and 'csType :> TBase  and 'fsType : equality> (x: 'fsType) = 
        let bytes = x |> serialize 
        use memBuffer = new TMemoryBuffer(bytes)
        let protR = TBinaryProtocol(memBuffer)
        let csVal = new 'csType()
        csVal.Read(protR)
        use memBuffer = new TMemoryBuffer()
        let protW = TBinaryProtocol(memBuffer)
        csVal.Write(protW)
        let newX = memBuffer.GetBuffer() |> deserialize
        x = newX
        
    [<Property>]
    let ``Test2 round-trips`` (x:Test2) = roundtrips x

    [<Property>]
    let ``Test2 round-trips via C#`` (x:Test2) = roundtripsVia<Test2, ThriftSharp.Test.ThriftGenerated.Test2> x

    [<Property>]
    let ``Test3 round-trips`` (x:string) =
        let t3 = { Test3.A = x; B = MyClass(x) }
        roundtrips t3
    [<Property>]
    let ``Test3 round-trips via C#`` (x:string) =
        let t3 = { Test3.A = x; B = MyClass(x) }
        roundtripsVia<Test3, ThriftSharp.Test.ThriftGenerated.Test3> t3

    [<Property(Arbitrary=[|typeof<Generators>|])>]
    let ``Union round-trips`` (x:Foo) = roundtrips x

    [<Property(Arbitrary=[|typeof<Generators>|])>]
    let ``Union round-trips via C#`` (x:Foo) = roundtripsVia<Foo, ThriftSharp.Test.ThriftGenerated.Foo>  x

