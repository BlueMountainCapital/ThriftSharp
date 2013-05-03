namespace ThriftSharp

open Microsoft.FSharp.Reflection
open Thrift.Protocol
open Thrift.Transport
open System
open TypeHelpers

module DynamicThrift = 

    let private fieldIds = Seq.initInfinite int16

    let rec thriftType = function
        | Bool      -> TType.Bool
        | Byte      -> TType.Byte
        | Double    -> TType.Double
        | Int16     -> TType.I16
        | Int32     -> TType.I32
        | Int64     -> TType.I64
        | String    -> TType.String
        | Enum      -> TType.I32
        | Option(t) -> thriftType t
        | Array(_) 
        | List(_)   -> TType.List
        | Set(_)    -> TType.Set
        | Map(_)    -> TType.Map
        | Tuple     -> TType.Struct
        | Union     -> TType.Struct
        // We put Record last because other types are represented as records
        | Record    -> TType.Struct
        | ty        -> failwithf "Unsupported type %s" ty.Name

    let rec thriftWriter ty : TProtocol -> obj -> unit = 
        match ty with
        | Bool      -> fun (prot: TProtocol) o -> prot.WriteBool(unbox o)
        | Byte      -> fun (prot: TProtocol) o -> prot.WriteByte(unbox o)
        | Double    -> fun (prot: TProtocol) o -> prot.WriteDouble(unbox o)
        | Int16     -> fun (prot: TProtocol) o -> prot.WriteI16(unbox o)
        | Int32     -> fun (prot: TProtocol) o -> prot.WriteI32(unbox o)
        | Int64     -> fun (prot: TProtocol) o -> prot.WriteI64(unbox o)
        | String    -> fun (prot: TProtocol) o -> prot.WriteString(unbox o)
        | Enum      -> fun (prot: TProtocol) o -> prot.WriteI32((o :?> IConvertible).ToInt32(null))
        | Array(t) 
        | List(t)   -> seqWriter t
        | Option(t) -> optionWriter ty t
        | Set(t)    -> setWriter t
        | Map(k,v)  -> mapWriter k v
        | Union     -> unionWriter ty
        | Tuple     -> tupleWriter ty
        // We put Record last because other types are represented as records
        | Record    -> recordWriter ty      
        | ty        -> failwithf "Unsupported type %s" ty.Name

    and private mapWriter keyType valType = 
        // Map implements IEnumerable, exposing KeyValuePair<K,V>.  We serialize by enumerating those
        // and accessing keys and values by reflection.
        // The same code should work for serializing a .NET Dictionary
        let keyTType = thriftType keyType
        let valTType = thriftType valType
        let kvpType = typedefof<System.Collections.Generic.KeyValuePair<_,_>>.MakeGenericType([| keyType ; valType |])
        let keyReader = kvpType.GetProperty("Key").GetGetMethod()
        let valReader = kvpType.GetProperty("Value").GetGetMethod()
        let keyWriter = thriftWriter keyType
        let valWriter = thriftWriter valType

        fun (prot: TProtocol) (o: obj) ->
            let s = (o :?> Collections.IEnumerable) |> Seq.cast        
            prot.WriteMapBegin(TMap(keyTType, valTType, Seq.length s))
            for elem in s do
                keyReader.Invoke(elem, [||]) |> keyWriter prot
                valReader.Invoke(elem, [||]) |> valWriter prot
            prot.WriteMapEnd()        
        
    // Option types are used to represent optional fields
    and private optionWriter t containedType =
        let someReader = FSharpType.GetUnionCases(t) |> Array.find (fun ci -> ci.Name = "Some")
                                                     |> FSharpValue.PreComputeUnionReader
        let valWriter = thriftWriter containedType

        fun (prot: TProtocol) o ->
            // We take advantage of the fact that None is represented as null
            // For None, we just write nothing
            if o <> null then
                let vals = someReader o
                valWriter prot vals.[0]         

    and private seqWriter elemType =     
        let elemTType = thriftType elemType
        let elemWriter = thriftWriter elemType
        fun (prot: TProtocol) o ->
            let elems = Seq.cast (o :?> Collections.IEnumerable)
            prot.WriteListBegin(new TList(elemTType, Seq.length elems))
            elems |> Seq.iter (elemWriter prot)
            prot.WriteListEnd()

    and private setWriter elemType =
        let elemTType = thriftType elemType
        let elemWriter = thriftWriter elemType
        fun (prot: TProtocol) o ->
            let elems = Seq.cast (o :?> Collections.IEnumerable)
            prot.WriteSetBegin(new TSet(elemTType, Seq.length elems))
            elems |> Seq.iter (elemWriter prot)
            prot.WriteSetEnd()
    
    and private tupleWriter t =
        let tupleElems = FSharpType.GetTupleElements(t)
        let structInfo = [ for idx, ty in Seq.zip fieldIds tupleElems -> (idx.ToString(), ty, idx) ]
        let tupleReader = FSharpValue.PreComputeTupleReader(t)
        writeStruct t.Name structInfo tupleReader

    and private recordWriter t = 
        let recFields = FSharpType.GetRecordFields(t)
        let structInfo = [ for idx, pi in Seq.zip fieldIds recFields -> (pi.Name, pi.PropertyType, idx) ]
        let recReader = FSharpValue.PreComputeRecordReader(t)
        writeStruct t.Name structInfo recReader

    and private unionWriter (t: System.Type) : TProtocol -> obj -> unit =    
        let tagReader = FSharpValue.PreComputeUnionTagReader(t)
        let cases = 
            [| for ci in FSharpType.GetUnionCases(t) ->
                let fld, writer = 
                    match ci.GetFields() with
                    | [| pi |] -> TField(ci.Name, thriftType pi.PropertyType, int16 ci.Tag), thriftWriter pi.PropertyType
                    | _ -> failwithf "ThriftWriter is not compatible with type %s - union cases must have exactly one data value" t.Name

                let unionReader = FSharpValue.PreComputeUnionReader ci
                let reader = fun o -> match unionReader o with
                                      | [| v |] -> v 
                                      | _       -> failwithf "CaseReader failed to return correct number of elements"
            
                fld, reader, writer
            |] 
        let name = t.Name

        fun (prot: TProtocol) o ->
            let tag = tagReader o
            let fld, unionCaseReader, thriftWriter = cases.[tag]
            let value = unionCaseReader o        
            prot.WriteStructBegin(TStruct(name))
            prot.WriteFieldBegin(fld)
            thriftWriter prot value 
            prot.WriteFieldEnd() 
            prot.WriteFieldStop()
            prot.WriteStructEnd()

    and private writeStruct name structInfo reader =
        let fieldWriters = [
            for name, ty, idx in structInfo ->
                let field = TField(name, thriftType ty, idx)
                let fieldWriter = thriftWriter ty             
                fun (prot: TProtocol) o ->
                    if (o <> null) then
                        prot.WriteFieldBegin(field)
                        fieldWriter prot o
                        prot.WriteFieldEnd()                        
        ]
       
        fun (oprot: TProtocol) value ->
            let values = reader value
            let struc = new TStruct(name)
            oprot.WriteStructBegin(struc)
            for (v, writer) in Seq.zip values fieldWriters do
                writer oprot v
            oprot.WriteFieldStop()
            oprot.WriteStructEnd()

    let rec thriftReader ty = 
        match ty with
        | Bool      -> fun (prot: TProtocol) -> prot.ReadBool() |> box
        | Byte      -> fun (prot: TProtocol) -> prot.ReadByte() |> box
        | Double    -> fun (prot: TProtocol) -> prot.ReadDouble() |> box
        | Int16     -> fun (prot: TProtocol) -> prot.ReadI16() |> box
        | Int32     -> fun (prot: TProtocol) -> prot.ReadI32() |> box
        | Int64     -> fun (prot: TProtocol) -> prot.ReadI64() |> box
        | String    -> fun (prot: TProtocol) -> prot.ReadString() |> box
        | Enum      -> fun (prot: TProtocol) -> Enum.ToObject(ty, prot.ReadI32())
        | Option(t) -> optionReader ty t
        | Array(t)  -> arrayReader ty t
        | List(t)   -> listReader ty t
        | Set(t)    -> setReader ty t
        | Map(k,v)  -> mapReader ty k v
        | Union     -> unionReader ty
        | Tuple     -> tupleReader ty
        // We put Record last because other types are represented as Records
        | Record    -> recordReader ty
        | ty        -> failwithf "Unsupported type %s" ty.Name

    and private mapReader mapType keyType valType = 
        // We read a map by constructing an array of tuples, then invoking the Map constructor
        let tupleType = FSharpType.MakeTupleType([| keyType; valType |])
        let tupleCtor = FSharpValue.PreComputeTupleConstructor(tupleType)
        let keyReader = thriftReader keyType
        let valReader = thriftReader valType
        let seqType = typedefof<seq<_>>.MakeGenericType([|tupleType|])
        let mapCtor = mapType.GetConstructor([|seqType|])

        fun (prot: TProtocol) ->
            let tmap = prot.ReadMapBegin()
            let arr = Array.CreateInstance(tupleType, tmap.Count)
            for i = 0 to tmap.Count-1 do
                let elem = tupleCtor [| keyReader prot; valReader prot |]
                arr.SetValue(elem, i)
            prot.ReadMapEnd()
            mapCtor.Invoke([|arr|])

    and private optionReader t containedType =
        let someCtor = FSharpType.GetUnionCases(t) |> Array.find (fun ci -> ci.Name = "Some")
                                                   |> FSharpValue.PreComputeUnionConstructor
                       
        let valReader = thriftReader containedType

        // We are only going to get called if we actually need to read a sum - otherwise we will just be skipped
        fun (prot: TProtocol) ->
            someCtor [| valReader prot |]
        
    and private arrayReader ty elemType : TProtocol -> obj =             
        let elemTType = thriftType elemType
        let elemsReader = readElems elemType
        fun (prot: TProtocol) ->
            let tlist = prot.ReadListBegin()
            if tlist.ElementType <> elemTType then
                failwithf "Expecting list of type %A but read %A" elemTType tlist.ElementType                
            let arr = elemsReader prot tlist.Count         
            prot.ReadListEnd()
            upcast arr

    and private readElems elemType =    
        let elemReader = thriftReader elemType    
        fun (prot: TProtocol) (count: int) ->
            let arr = Array.CreateInstance(elemType, count)
            for i in [0..count-1] do
                let elem = elemReader prot
                arr.SetValue(elem, i)
            arr

    and private listReader ty elemType : TProtocol -> obj =
        let arrReader = arrayReader ty elemType
        // This is rather ugly, but I can find no other way of invoking this static method via reflection
        let listCtorMethodInfo = Type.GetType("Microsoft.FSharp.Collections.ListModule, FSharp.Core")   
                                     .GetMethod("OfArray")
                                     .MakeGenericMethod([|elemType|])
        let listCtor = fun o -> listCtorMethodInfo.Invoke(null, [|o|])
        arrReader >> listCtor

    and private setReader ty elemType : TProtocol -> obj =         
        let elemTType = thriftType elemType
        let elemsReader = readElems elemType
        // This is rather ugly, but I can find no other way of invoking this static method via reflection
        let setCtorMethodInfo = Type.GetType("Microsoft.FSharp.Collections.SetModule, FSharp.Core")   
                                     .GetMethod("OfArray")
                                     .MakeGenericMethod([|elemType|])
        let setCtor = fun o -> setCtorMethodInfo.Invoke(null, [|o|])

        fun (prot: TProtocol) ->
            let tset = prot.ReadSetBegin()
            if tset.ElementType <> elemTType then
                failwithf "Expecting set of type %A but read %A" elemTType tset.ElementType
        
            let arr = elemsReader prot tset.Count
            prot.ReadSetEnd()
            setCtor arr

    and private tupleReader t : TProtocol -> obj = 
        let tupleInfo = FSharpType.GetTupleElements(t)
        let ctor = FSharpValue.PreComputeTupleConstructor t
        structReader tupleInfo ctor

    and private recordReader t : TProtocol -> obj =
        let typeInfo = [| for pi in FSharpType.GetRecordFields(t) -> pi.PropertyType |]
        let ctor = FSharpValue.PreComputeRecordConstructor t
        structReader typeInfo ctor

    and private unionReader t : TProtocol -> obj  =    
        let cases = 
            [| for ci in FSharpType.GetUnionCases t ->                         
                match ci.GetFields() with
                | [| pi |] -> FSharpValue.PreComputeUnionConstructor(ci), thriftReader pi.PropertyType
                | _ -> failwithf "Thrift reader is not compatible with type %s - union cases must have exactly one data value" t.Name
            |] 

        fun (prot: TProtocol) ->
            prot.ReadStructBegin() |> ignore
            let field = prot.ReadFieldBegin()
            let ctor, reader = cases.[int field.ID]
                      
            let value = reader prot
            prot.ReadFieldEnd()
            if prot.ReadFieldBegin().Type <> TType.Stop then
                raise <| TProtocolException(TProtocolException.INVALID_DATA)         
            prot.ReadStructEnd()

            ctor [| value |]

    and private structReader typeInfo ctor : TProtocol -> obj = 
        let fieldReaders = [|
            for ty in typeInfo ->
                let ttype = thriftType ty
                let reader = thriftReader ty
                fun (iprot: TProtocol) (fldType: TType) ->
                    if fldType <> ttype then
                        TProtocolUtil.Skip(iprot, fldType)
                        null
                    else
                        reader iprot
        |]

        let numFields = typeInfo.Length

        fun (iprot: TProtocol)  ->
            let data = Array.zeroCreate numFields
        
            iprot.ReadStructBegin() |> ignore

            let rec readFields() = 
                let fld = iprot.ReadFieldBegin()
                if fld.Type <> TType.Stop then
                    let id = int fld.ID
                
                    if id < 0 || id >= numFields then
                        TProtocolUtil.Skip(iprot, fld.Type)
                    data.[id] <- fieldReaders.[id] iprot fld.Type
                    iprot.ReadFieldEnd()
                    readFields()  
  
            readFields()

            iprot.ReadStructEnd() |> ignore
            unbox <| ctor data

    let readerFor<'t> = thriftReader typeof<'t> >> unbox<'t>
    let writerFor<'t> : TProtocol -> 't -> unit = thriftWriter typeof<'t>

/// Wrapper class used to serialize & deserialize an F# type using Thrift
type TBaseWrapper<'t>(t) =
    static let reader = DynamicThrift.readerFor<'t>
    static let writer = DynamicThrift.writerFor<'t>
    let mutable value = t

    member this.Value = value

    new() = TBaseWrapper(Unchecked.defaultof<'t>)

    interface TBase with
        member x.Read(prot: TProtocol) = 
            let v = reader prot            
            value <- v
        member x.Write(prot: TProtocol) =                 
            writer prot value
