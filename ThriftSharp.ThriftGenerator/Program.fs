open System
open Thrift.Protocol
open ThriftSharp
open TypeHelpers

module ThriftGenerator =
    let private fieldIds = Seq.initInfinite (fun i -> int16 (i+1))

    /// Return all types that must be turned into a Thrift structure
    /// This might include duplicates, and will be in depth-first order
    let rec private getThriftStructureTypes tryGetProxy ty : System.Type list =
        let getTypes = getThriftStructureTypes tryGetProxy

        match tryGetProxy ty with 
            | Some { ProxyType = proxyType } -> getTypes proxyType
            | None ->
                match ty with
                | Option(t)
                | Array(t)
                | List(t) 
                | Set(t)        -> getTypes t
                | Enum          -> [ ty ]
                | Map(k,v)      -> getTypes k @ getTypes v
                | Union(ci)     -> [ for c in ci do for pi in c.GetFields() do yield! getTypes pi.PropertyType
                                     yield ty ]
                | Tuple(es)     -> [ for elem in es do yield! getTypes elem
                                     yield ty ]
                // We put Record last because other types are represented as records
                | Record(flds)  -> [ for fld in flds do yield! getTypes fld.PropertyType
                                     yield ty ]      
                | _             -> List.empty

    let private stripGenericTickPart (t:string) = t.Substring(0, t.IndexOf('`'))   
        
    /// Format the name for the given type, when declared or referenced in the .thrift file
    let rec private typeName tryGetProxy ty : string = 
        let getTypeName = typeName tryGetProxy

        match tryGetProxy ty with 
            | Some { ProxyType = proxyType } -> getTypeName proxyType
            | None ->
                match ty with
                | Bool      -> "bool"
                | Byte      -> "byte"
                | Double    -> "double"
                | Int16     -> "i16"
                | Int32     -> "i32"
                | Int64     -> "i64"
                | String    -> "string"
                | Enum      -> ty.Name
                | Array(t) 
                | List(t)   -> sprintf "list<%s>" <| getTypeName t
                | Set(t)    -> sprintf "set<%s>" <| getTypeName t
                | Map(k,v)  -> sprintf "map<%s,%s>" (getTypeName k) (getTypeName v)
                // This will only match when this is part of the typename for a tuple.
                // When it is a struct field, the optionality will be expressed via optional/required 
                | Option(t) -> sprintf "optional_%s" <| getTypeName t
                // Match records, unions that have generic params
                | Generic(ps) -> String.concat "_" [ yield stripGenericTickPart ty.Name
                                                     for parm in ps -> getTypeName parm ] 
                | ty          -> ty.Name
 
   
    let private structDefinition tryGetProxy ty flds = [
        yield sprintf "struct %s {" (typeName tryGetProxy ty)
        for id, (fldType, fldName) in Seq.zip fieldIds flds -> 
            let optReq, ft = match fldType with 
                             | Option(t) -> "optional", t 
                             | t         -> "required", t
            sprintf "\t%d: %s %s %s" id optReq (typeName tryGetProxy ft) fldName
        yield "}" ]
 
    let typeDefinition tryGetProxy ty : string = 
        match ty with
        | Enum         -> [ yield sprintf "enum %s {" (typeName tryGetProxy ty)
                            for name, value in Seq.zip (ty.GetEnumNames()) (ty.GetEnumValues() |> Seq.cast<IConvertible>) ->
                                sprintf "\t%s = %d" name (value.ToInt32(null))
                            yield "}" ]
        | Record(flds) -> structDefinition tryGetProxy ty [ for fld in flds -> fld.PropertyType, fld.Name ]
        | Tuple(elems) -> structDefinition tryGetProxy ty [ for id, ty in Seq.zip fieldIds elems -> ty, sprintf "Item%d" id ]
        | Union(cases) -> [ yield sprintf "union %s {" (typeName tryGetProxy ty)
                            for case in cases ->
                                match case.GetFields() with
                                | [| fld |] -> sprintf "\t%d: %s %s" (case.Tag+1) (typeName tryGetProxy fld.PropertyType) case.Name
                                | flds -> failwithf "Only single-field union cases are supported - type %s case %s has %d" ty.Name case.Name flds.Length
                            yield "}" ]
        | _ -> List.empty

        |> String.concat "\n"

    let definitionsFromRootTypes tryGetProxy types : string =
        types |> Seq.map (getThriftStructureTypes tryGetProxy)
              |> Seq.concat
              |> Seq.distinct
              |> Seq.map (typeDefinition tryGetProxy)
              |> String.concat "\n\n"
 

[<EntryPoint>]
let main argv =
    match argv with
    | [| dllName; nsName; outFileName; outNs |] -> 
            let assem = System.Reflection.Assembly.LoadFile(dllName)
            let types = [ for ty in assem.ExportedTypes do
                            if ty.Namespace = nsName && not(ty.ContainsGenericParameters) && not(ty.IsNested) then
                                yield ty ]
            let proxy = [ for ty in assem.DefinedTypes do
                            if ty.Namespace = nsName && ty.Name = "Proxies" then
                                yield ty.GetMethod("tryGetProxy") ]
                        |> List.head
            let tryGetProxy ty = proxy.Invoke(null, [| ty |]) |> unbox
            let nsDef = sprintf "namespace csharp %s\n\n" outNs
            let src = ThriftGenerator.definitionsFromRootTypes tryGetProxy types
            System.IO.File.WriteAllText(outFileName, nsDef + src)
            0

    | _ -> printfn "USAGE: ThriftSharp.ThriftGenerator dllName inputNamespace outFileName outputNamespace"
           -1
