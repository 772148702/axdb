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
  internal Int? transId
  private Bool autoCommit := true

  static const Log log := Executor#.pod.log

  new make(Engine engine) {
    this.engine = engine
  }

  Obj?[] exeSql(Str sql, Int? transId_ := null) {
    log.debug("exeSql: $sql")
    this.transId = transId_
    parser := Parser(sql)
    unit := parser.parse

    if (transId == null) {
      if (unit.stmts.first.typeof == TransStmt#) {
        autoCommit = false
      } else {
        autoCommit = true
        transId = engine.transact(null, TransState.begin)
      }
    } else {
      autoCommit = false
    }

    res := [,]
    unit.stmts.each {
      //echo("exe: $it")
      res.add(exeStmt(it))
    }

    tryCommit
    return res
  }

  private Obj? exeStmt(Stmt stmt) {
    switch (stmt.typeof) {
      case CreateStmt#:
        return engine.createTable(transId, stmt)
      case InsertStmt#:
        return insert(stmt)
      case SelectStmt#:
        p := plan(stmt)
        return query(p)
      case TransStmt#:
        return trans(stmt)
      default:
        echo("TODO $stmt")
    }
    return null
  }

  private Void tryCommit() {
    if (!autoCommit || transId == null) return
    engine.transact(transId, TransState.commit)
    transId = null
  }

  private Obj? trans(TransStmt stmt) {
    if (stmt.state == TransState.begin) {
      tryCommit
    }

    id := engine.transact(stmt.transId, stmt.state)
    if (stmt.state == TransState.begin) {
      transId = id
    } else {
      transId = null
      autoCommit = true
    }
    return id
  }

  Plan? plan(SelectStmt stmt) {
    if (stmt.cond == null) {
      return ScanPlan { table = stmt.tables.first }
    }
    keyExpr := stmt.cond.right as LiteralExpr
    keybuf := BufUtil.strToBuf(keyExpr.val)
    plan := IdSearchPlan {
      it.table = stmt.tables.first
      it.key = keybuf
      it.executor = this
    }
    return plan
  }

  private [Str:Obj]? query(Plan plan) {
    record := plan.next
    if (record == null) return null
    return record.vals
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