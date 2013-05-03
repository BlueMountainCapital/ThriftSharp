namespace ThriftSharp

module TypeHelpers = 

    open Microsoft.FSharp.Reflection
    open System

    let private decomposeSingleGeneric<'genericType> (ty: System.Type) =
        if ty.IsConstructedGenericType && ty.GetGenericTypeDefinition() = typedefof<'genericType> then Some(ty.GenericTypeArguments.[0]) else None
 
    // Some active patterns to make matching on System.Types more pleasing
    let (|Bool|_|)   (ty: System.Type) = if ty = typeof<bool> then Some() else None
    let (|Byte|_|)   (ty: System.Type) = if ty = typeof<byte> then Some() else None
    let (|Double|_|) (ty: System.Type) = if ty = typeof<double> then Some() else None
    let (|Int16|_|)  (ty: System.Type) = if ty = typeof<Int16> then Some() else None
    let (|Int32|_|)  (ty: System.Type) = if ty = typeof<Int32> then Some() else None
    let (|Int64|_|)  (ty: System.Type) = if ty = typeof<Int64> then Some() else None
    let (|String|_|) (ty: System.Type) = if ty = typeof<string> then Some() else None
    let (|Record|_|) (ty: System.Type) = if FSharpType.IsRecord(ty) then Some() else None
    let (|Option|_|) (ty: System.Type) = decomposeSingleGeneric<Option<_>> ty
    let (|Union|_|)  (ty: System.Type) = if FSharpType.IsUnion(ty) then Some() else None
    let (|Tuple|_|)  (ty: System.Type) = if FSharpType.IsTuple(ty) then Some() else None
    let (|Array|_|)  (ty: System.Type) = if ty.IsArray then Some(ty.GetElementType()) else None
    let (|List|_|)   (ty: System.Type) = decomposeSingleGeneric<list<_>> ty
    let (|Set|_|)    (ty: System.Type) = decomposeSingleGeneric<Set<_>> ty
    let (|Map|_|)    (ty: System.Type) = if ty.IsConstructedGenericType && ty.GetGenericTypeDefinition() = typedefof<Map<_,_>> then 
                                            Some(ty.GenericTypeArguments.[0], ty.GenericTypeArguments.[1]) else None
    let (|Enum|_|)   (ty: System.Type) = if ty.IsEnum then Some() else None

