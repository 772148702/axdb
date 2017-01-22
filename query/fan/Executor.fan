//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using axdbStore
using util

class Executor {

  Engine engine
  Int? transId

  new make(Engine engine) {
    this.engine = engine
  }

  Obj?[] exeSql(Str sql) {
    parser := Parser(sql)
    unit := parser.parse

    res := [,]
    unit.stmts.each {
      echo("exe: $it")
      res.add(exeStmt(it))
    }
    return res
  }

  private Obj? exeStmt(Stmt stmt) {
    switch (stmt.typeof) {
      case CreateStmt#:
        return engine.createTable(transId, stmt)
      case InsertStmt#:
        return insert(stmt)
      case SelectStmt#:
        return query(stmt)
      default:
        echo("TODO $stmt")
    }
    return null
  }

  private Str:Obj? query(SelectStmt stmt) {
    keyExpr := stmt.cond.right as LiteralExpr
    keybuf := BufUtil.strToBuf(keyExpr.val)
    valbuf := engine.search(transId, stmt.tables.first, keybuf)
    data := JsonInStream(valbuf.in).readJson
    return data
  }

  private Bool insert(InsertStmt stmt) {
    map := Str:Obj[:]
    for (i:=0; i<stmt.fields.size; ++i) {
      map[stmt.fields[i]] = stmt.values[i]
    }
    tab := engine.tableMeta[stmt.table]
    key := map[tab.key]
    keybuf := BufUtil.strToBuf(key)

    valbuf := Buf()
    JsonOutStream(valbuf.out).writeJson(map)
    valbuf.flip

    engine.insert(transId, stmt.table, keybuf, valbuf)
    return true
  }
}