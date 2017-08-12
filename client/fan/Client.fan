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
  private Int transId := -1

  new make(Uri host) {
    this.host = host.plusSlash
  }

  Void begin() {
    if (transId != -1) {
      rollback
    }
    Str[]? res := exeSql("begin TRANSACTION")
    transId = res[0].toInt
  }

  Void commit() {
    exeSql("commit TRANSACTION")
    transId = -1
  }

  Void rollback() {
    exeSql("abort TRANSACTION")
    transId = -1
  }

  Void createTable(Str table) {
    exeSql("CREATE TABLE $table(id text NOT NULL, val text, PRIMARY KEY (id))")
  }

  Void dropTable(Str table) {
    exeSql("Drop table $table")
  }

  Str get(Str table, Str key) {
    skey := key.replace("'", "''")
    Obj[]? res := exeSql("select * FROM $table where id ='$skey'")
    return res[0]->get("val")
  }

  Void set(Str table, Str key, Str val) {
    skey := key.replace("'", "''")
    sval := val.replace("'", "''")
    exeSql("INSERT INTO $table(id, val) VALUES ('$skey','$sval')")
  }

  Obj? exeSql(Str sql) {
    sql = "$transId:$sql"
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


