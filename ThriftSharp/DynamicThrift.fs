namespace ThriftSharp

open Microsoft.FSharp.Reflection
open Thrift.Protocol
open Thrift.Transport
open System
open System.Reflection
open TypeHelpers

module DynamicThrift = 

    let private fieldIds = Seq.initInfinite int16
    let private bindingFlags = BindingFlags.Public ||| BindingFlags.NonPublic

    // Option types are used to represent optional fields
    let private optionWriter t containedType thriftWriter =
        let someReader = let someCase = FSharpType.GetUnionCases(t, bindingFlags) 
                                        |> Array.find (fun ci -> ci.Name = "Some")
                         FSharpValue.PreComputeUnionReader(someCase, bindingFlags)
        let ttype, valWriter = thriftWriter containedType

        ttype, fun (prot: TProtocol) o ->
                // We take advantage of the fact that None is represented as null
                // For None, we just write nothing
                if o <> null then
                    let vals = someReader o
                    valWriter prot vals.[0]         

    let private seqWriter elemType thriftWriter =     
        let elemTType, elemWriter = thriftWriter elemType
        fun (prot: TProtocol) (o: obj) ->
            let elems = Seq.cast (o :?> Collections.IEnumerable)
            prot.WriteListBegin(new TList(elemTType, Seq.length elems))
            elems |> Seq.iter (elemWriter prot)
            prot.WriteListEnd()

    let private setWriter elemType thriftWriter =
        let elemTType, elemWriter = thriftWriter elemType
        fun (prot: TProtocol) (o: obj) ->
            let elems = Seq.cast (o :?> Collections.IEnumerable)
            prot.WriteSetBegin(new TSet(elemTType, Seq.length elems))
            elems |> Seq.iter (elemWriter prot)
            prot.WriteSetEnd()
    
    let private mapWriter keyType valType thriftWriter = 
        // Map implements IEnumerable, exposing KeyValuePair<K,V>.  We serialize by enumerating those
        // and accessing keys and values by reflection.
        // The same code should work for serializing a .NET Dictionary
        let kvpType = typedefof<System.Collections.Generic.KeyValuePair<_,_>>.MakeGenericType([| keyType ; valType |])
        let keyReader = kvpType.GetProperty("Key").GetGetMethod()
        let valReader = kvpType.GetProperty("Value").GetGetMethod()
        let keyTType, keyWriter = thriftWriter keyType
        let valTType, valWriter = thriftWriter valType

        fun (prot: TProtocol) (o: obj) ->
            let s = (o :?> Collections.IEnumerable) |> Seq.cast        
            prot.WriteMapBegin(TMap(keyTType, valTType, Seq.length s))
            for elem in s do
                keyReader.Invoke(elem, [||]) |> keyWriter prot
                valReader.Invoke(elem, [||]) |> valWriter prot
            prot.WriteMapEnd()        

    let private unionWriter t thriftWriter =    
        let tagReader = FSharpValue.PreComputeUnionTagReader(t, bindingFlags)
        let cases = 
            [| for ci in FSharpType.GetUnionCases(t, bindingFlags) ->
                let fld, writer = 
                    match ci.GetFields() with
                    | [| pi |] -> let ttype, writer = thriftWriter pi.PropertyType
                                  TField(ci.Name, ttype, int16 ci.Tag), writer
                    | _ -> failwithf "ThriftWriter is not compatible with type %s - union cases must have exactly one data value" t.Name

                let unionReader = FSharpValue.PreComputeUnionReader(ci, bindingFlags)
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
    
    let private writeStruct name structInfo reader thriftWriter =
        let fieldWriters = [
            for name, ty, idx in structInfo ->
                let ttype, fieldWriter = thriftWriter ty 
                let field = TField(name, ttype, idx)          
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
        
    let private tupleWriter t thriftWriter =
        let tupleElems = FSharpType.GetTupleElements(t)
        let structInfo = [ for idx, ty in Seq.zip fieldIds tupleElems -> (idx.ToString(), ty, idx) ]
        let tupleReader = FSharpValue.PreComputeTupleReader(t)
        writeStruct t.Name structInfo tupleReader thriftWriter

    let private recordWriter t thriftWriter = 
        let recFields = FSharpType.GetRecordFields(t, bindingFlags)
        let structInfo = [ for idx, pi in Seq.zip fieldIds recFields -> (pi.Name, pi.PropertyType, idx) ]
        let recReader = FSharpValue.PreComputeRecordReader(t, bindingFlags)
        writeStruct t.Name structInfo recReader thriftWriter

    let rec thriftWriter customWriter ty : (TType * (TProtocol -> obj -> unit)) =
        // Function to pass into other writers to allow them to get a writers for another type
        let getWriter = thriftWriter customWriter

        match customWriter ty with 
            | Some (ttype, writer) -> (ttype, writer)
            | None ->
                match ty with
                | Bool      -> TType.Bool,      fun (prot: TProtocol) o -> prot.WriteBool(unbox o)
                | Byte      -> TType.Byte,      fun (prot: TProtocol) o -> prot.WriteByte(unbox o)
                | Double    -> TType.Double,    fun (prot: TProtocol) o -> prot.WriteDouble(unbox o)
                | Int16     -> TType.I16,       fun (prot: TProtocol) o -> prot.WriteI16(unbox o)
                | Int32     -> TType.I32,       fun (prot: TProtocol) o -> prot.WriteI32(unbox o)
                | Int64     -> TType.I64,       fun (prot: TProtocol) o -> prot.WriteI64(unbox o)
                | String    -> TType.String,    fun (prot: TProtocol) o -> prot.WriteString(unbox o)
                | Enum      -> TType.I32,       fun (prot: TProtocol) o -> prot.WriteI32((o :?> IConvertible).ToInt32(null))
                | Option(t) -> optionWriter ty t getWriter
                | Array(t) 
                | List(t)   -> TType.List,      seqWriter t getWriter
                | Set(t)    -> TType.Set,       setWriter t getWriter
                | Map(k,v)  -> TType.Map,       mapWriter k v getWriter
                | Union     -> TType.Struct,    unionWriter ty getWriter
                | Tuple     -> TType.Struct,    tupleWriter ty getWriter
                // We put Record last because other types are represented as records
                | Record    -> TType.Struct,    recordWriter ty getWriter      
                | ty        -> failwithf "Unsupported type %s" ty.Name

    let private optionReader t containedType thriftReader =
        let someCtor = let someCase = FSharpType.GetUnionCases(t, bindingFlags) |> Array.find (fun ci -> ci.Name = "Some")
                       FSharpValue.PreComputeUnionConstructor(someCase, bindingFlags)
                       
        let valType, valReader = thriftReader containedType

        // We are only going to get called if we actually need to read a sum - otherwise we will just be skipped
        valType, fun (prot: TProtocol) -> someCtor [| valReader prot |]

    let private readElems elemType elemReader =    
        fun (prot: TProtocol) (count: int) ->
            let arr = Array.CreateInstance(elemType, count)
            for i in [0..count-1] do
                let elem = elemReader prot
                arr.SetValue(elem, i)
            arr

    let private arrayReader ty elemType thriftReader =             
        let elemTType, elemReader = thriftReader elemType
        let elemsReader = readElems elemType elemReader
        fun (prot: TProtocol) ->
            let tlist = prot.ReadListBegin()
            if tlist.ElementType <> elemTType then
                failwithf "Expecting list of type %A but read %A" elemTType tlist.ElementType                
            let arr = elemsReader prot tlist.Count         
            prot.ReadListEnd()
            box arr

    let private listReader ty elemType thriftReader =
        let arrReader = arrayReader ty elemType thriftReader
        // This is rather ugly, but I can find no other way of invoking this static method via reflection
        let listCtorMethodInfo = Type.GetType("Microsoft.FSharp.Collections.ListModule, FSharp.Core")   
                                     .GetMethod("OfArray")
                                     .MakeGenericMethod([|elemType|])
        let listCtor = fun o -> listCtorMethodInfo.Invoke(null, [|o|])
        arrReader >> listCtor

    let private setReader ty elemType thriftReader =         
        let elemTType, elemReader = thriftReader elemType
        let elemsReader = readElems elemType elemReader
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

    let private mapReader (mapType: Type) keyType valType thriftReader = 
        // We read a map by constructing an array of tuples, then invoking the Map constructor
        let tupleType = FSharpType.MakeTupleType([| keyType; valType |])
        let tupleCtor = FSharpValue.PreComputeTupleConstructor(tupleType)
        let _, keyReader = thriftReader keyType
        let _, valReader = thriftReader valType
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

    let private unionReader t thriftReader  =    
        let cases = 
            [| for ci in FSharpType.GetUnionCases(t, bindingFlags) ->                         
                match ci.GetFields() with
                | [| pi |] -> FSharpValue.PreComputeUnionConstructor(ci, bindingFlags), thriftReader pi.PropertyType
                | _ -> failwithf "Thrift reader is not compatible with type %s - union cases must have exactly one data value" t.Name
            |] 

        fun (prot: TProtocol) ->
            prot.ReadStructBegin() |> ignore
            let field = prot.ReadFieldBegin()
            let ctor, (ttype, reader) = cases.[int field.ID]
                      
            let value = reader prot
            prot.ReadFieldEnd()
            if prot.ReadFieldBegin().Type <> TType.Stop then
                raise <| TProtocolException(TProtocolException.INVALID_DATA)         
            prot.ReadStructEnd()

            ctor [| value |]

    let private structReader typeInfo ctor thriftReader = 
        let fieldReaders = [|
            for ty in typeInfo ->
                let ttype, reader = thriftReader ty
                fun (iprot: TProtocol) (fldType: TType) ->
                    if fldType <> ttype then
                        TProtocolUtil.Skip(iprot, fldType)
                        null
                    else
                        reader iprot
        |]

        let numFields = Array.length typeInfo

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


    let private tupleReader t = 
        let tupleInfo = FSharpType.GetTupleElements(t)
        let ctor = FSharpValue.PreComputeTupleConstructor t
        structReader tupleInfo ctor

    let private recordReader t  =
        let typeInfo = [| for pi in FSharpType.GetRecordFields(t, bindingFlags) -> pi.PropertyType |]
        let ctor = FSharpValue.PreComputeRecordConstructor(t, bindingFlags)
        structReader typeInfo ctor

    let rec thriftReader customReader ty =
        // Function to pass into other readers to allow them to get a reader for another type
        let getReader = thriftReader customReader
         
        match customReader ty with
        | Some(reader) -> reader
        | None ->
            match ty with
            | Bool      -> TType.Bool,   fun (prot: TProtocol) -> prot.ReadBool() |> box
            | Byte      -> TType.Byte,   fun (prot: TProtocol) -> prot.ReadByte() |> box
            | Double    -> TType.Double, fun (prot: TProtocol) -> prot.ReadDouble() |> box
            | Int16     -> TType.I16,    fun (prot: TProtocol) -> prot.ReadI16() |> box
            | Int32     -> TType.I32,    fun (prot: TProtocol) -> prot.ReadI32() |> box
            | Int64     -> TType.I64,    fun (prot: TProtocol) -> prot.ReadI64() |> box
            | String    -> TType.String, fun (prot: TProtocol) -> prot.ReadString() |> box
            | Enum      -> TType.I32,    fun (prot: TProtocol) -> Enum.ToObject(ty, prot.ReadI32())
            | Option(t) -> optionReader ty t getReader
            | Array(t)  -> TType.List,   arrayReader ty t getReader
            | List(t)   -> TType.List,   listReader ty t getReader
            | Set(t)    -> TType.Set,    setReader ty t getReader
            | Map(k,v)  -> TType.Map,    mapReader ty k v getReader
            | Union     -> TType.Struct, unionReader ty getReader
            | Tuple     -> TType.Struct, tupleReader ty getReader
            // We put Record last because other types are represented as Records
            | Record    -> TType.Struct, recordReader ty getReader
            | ty        -> failwithf "Unsupported type %s" ty.Name
    
    let internal wrapReader (getCustomReader: Func<System.Type, Tuple<TType, Func<TProtocol, obj>>>) =
        match getCustomReader with
        | null -> fun _ -> None
        | f    -> fun t -> match getCustomReader.Invoke(t) with
                            | null -> None
                            | tpl -> Some(tpl.Item1, tpl.Item2.Invoke)

    /// C#-friendly wrapper to get a reader for a given type.
    /// getCustomReader is an optional function that can be used to provide type-specific reader
    let GetReaderFor(valType: System.Type, getCustomReader: Func<System.Type, Tuple<TType, Func<TProtocol, obj>>>) = 
        let customReader = wrapReader getCustomReader
        let ttype, reader = thriftReader customReader valType
        Func<TProtocol,obj>(reader)

    let internal wrapWriter (getCustomWriter: Func<System.Type, Tuple<TType, Action<TProtocol, obj>>>) =
        match getCustomWriter with
        | null -> fun _ -> None
        | f    -> fun t -> match getCustomWriter.Invoke(t) with
                            | null -> None
                            | tpl -> Some(tpl.Item1, fun prot o -> tpl.Item2.Invoke(prot, o))

    /// C#-friendly wrapper to get a writer for a given type.
    /// getCustomWriter is an optional function that can be used to provide type-specific writer
    let GetWriterFor(valType: System.Type, getCustomWriter) =
        let customWriter = wrapWriter getCustomWriter
        let ttype, writer = thriftWriter customWriter valType
        Action<TProtocol,obj>(writer)

    let readerFor<'t> getCustomReader = 
        let ttype, reader = thriftReader getCustomReader typeof<'t> 
        reader >> unbox<'t>
    
    let writerFor<'t> getCustomWriter = 
        let ttype, writer = thriftWriter getCustomWriter typeof<'t>
        fun (prot: TProtocol) (t: 't) -> writer prot (box t)

/// Wrapper class used to serialize & deserialize an F# type using Thrift
type TBaseWrapper<'t>(t: 't, getCustomReader : Type -> Option<TType * (TProtocol -> obj)>, getCustomWriter : Type -> Option<TType * (TProtocol -> obj -> unit)>) =
    let reader = lazy DynamicThrift.readerFor<'t> getCustomReader
    let writer = lazy DynamicThrift.writerFor<'t> getCustomWriter
    let mutable value = t

    member this.Value = value

    new() = TBaseWrapper(Unchecked.defaultof<'t>, (fun _ -> None), (fun _ -> None))
    new(t) = TBaseWrapper(t, (fun _ -> None), (fun _ -> None))
    new(getCustomReader) = TBaseWrapper(Unchecked.defaultof<'t>, getCustomReader, (fun _ -> None))
    new(t, getCustomWriter) = TBaseWrapper(t, (fun _ -> None), getCustomWriter)

    interface TBase with
        member x.Read(prot: TProtocol) = 
            let v = reader.Value prot            
            value <- v
        member x.Write(prot: TProtocol) =                 
            writer.Value prot value

