//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent
using web

const class RpcClient : Actor {
  const Uri uri
  private const Type type

  new make(Type refType, Uri u, ActorPool pool) : super.make(pool) {
    type = refType
    uri = u.plusSlash
  }

  override Obj? trap(Str name, Obj?[]? args := null) {
    Bool async := true
    if (name.startsWith("send_")) {
      name = name[5..-1]
      async = false
    }
    method := type.method(name)
    query := [Str:Str][:]
    method.params.each |v, i|{
      query[v.name] = args[i].toStr
    }
    reqUri := (uri + name.toUri).plusQuery(query)
    echo("req: $reqUri")

    if (!async) {
      return this.send(reqUri)
    }
    return request(reqUri)
  }

  private Str request(Uri uri) {
    Obj? res := null
    WebClient? c
    try {
      c = WebClient(uri)
      c.socketOptions { connectTimeout = 8sec; receiveTimeout = 8sec }
      res = c.getStr
    } catch (Err e) {
      echo("request err: $e.msg")
    }
    finally c?.close

    return res
  }

  protected override Obj? receive(Obj? msg) {
    //echo("send: $msg")
    Uri uri := msg
    Obj? res := request(uri)
    //echo("response: $res")
    return res
  }
}


