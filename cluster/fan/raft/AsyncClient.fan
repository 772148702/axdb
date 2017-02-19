//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent
using web

class AsyncClient {
  Uri uri
  static const Actor actor := WebActor()
  private Type type

  new make(Type refType, Uri u) {
    type = refType
    uri = u.plusSlash
  }

  override Obj? trap(Str name, Obj?[]? args := null) {
    Bool async := false
    if (name.startsWith("send_")) {
      name = name[5..-1]
      async = true
    }
    method := type.method(name)
    query := [Str:Str][:]
    method.params.each |v, i|{
      query[v.name] = args[i].toStr
    }
    reqUri := (uri + name.toUri).plusQuery(query)
    //echo("send req: $reqUri")

    if (!async) {
      return actor.send(reqUri)
    }
    Uri uri := reqUri
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
}

const class WebActor : Actor {
  new make() : super(ActorPool{}) {
  }
  protected override Obj? receive(Obj? msg) {
    echo("send: $msg")
    Uri uri := msg
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

    echo("response: $res")
    return res
  }
}

