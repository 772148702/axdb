//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent
using web

class Client {
  const Uri host
  private Int? transId

  new make(Uri host) {
    this.host = host.plusSlash
  }

  Void begin() {
    if (transId != null) {
      rollback
    }
    Str[]? res := exeRawSql(":begin TRANSACTION")
    transId = res[0].toInt
  }

  Void commit() {
    exeSql("commit TRANSACTION")
    transId = null
  }

  Void rollback() {
    exeSql("abort TRANSACTION")
    transId = null
  }

  Obj? exeSql(Str sql) {
    sql = transId != null ? "$transId:sql" : ":$sql"
    return exeRawSql(sql)
  }

  private Obj? exeRawSql(Str sql) {
    uri := host + `exeSql`
    uri = uri.plusQuery(["sql" : sql])
    res := request(uri)
    return res.in.readObj
  }

  private Str request(Uri uri) {
    //echo(uri)
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


