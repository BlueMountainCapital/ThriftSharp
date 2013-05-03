namespace ThriftSharp.Test

open Thrift.Transport
open System
open System.IO

/// A class that allows a MemoryStream to be used as a Thrift Transport
/// This exists in the HEAD of thrift but not the NuGet version
type TMemoryBuffer(byteStream) =
    inherit TTransport()

    let mutable isDisposed = false

    new() = new TMemoryBuffer(new MemoryStream())
    new(bytes: byte[]) = new TMemoryBuffer(new MemoryStream(bytes))

    override this.Open() = ()
    override this.Close() = ()
    override this.IsOpen = true
    override this.Read(buf, off, len) = byteStream.Read(buf, off, len)
    override this.Write(buf, off, len) = byteStream.Write(buf, off, len)
    override this.Dispose(disposing) =
        if not(isDisposed) && disposing && byteStream <> null then
            byteStream.Dispose()
            isDisposed <- true

    member this.GetBuffer() = byteStream.ToArray()
        