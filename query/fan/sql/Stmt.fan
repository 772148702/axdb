//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//
using axdbStore

** expression
class Expr {
}

** statment
class Stmt {
}

class CmpUnit {
  Stmt[] stmts := [,]

  Void add(Stmt s) { stmts.add(s) }

  Void dump() {
    stmts.each { echo(it) }
  }
}

class LiteralExpr : Expr {
  Obj val
  new make(Obj v) {
    val = v
  }

  override Str toStr() { "'$val'" }
}

class IdExpr : Expr {
  Str name
  new make(Str n) {
    name = n
  }

  override Str toStr() { "$name" }
}

** condition expression
class BinaryExpr : Expr {
  Expr left
  Expr right
  Str op

  new make(Obj l, Str op, Obj r) {
    left = l
    this.op = op
    right = r
  }

  override Str toStr() {
    "($left $op $right)"
  }
}

class AssignExpr {
  Str field
  Obj? value

  new make(Str f, Obj val) {
    field = f
    value = val
  }

  override Str toStr() {
    "$field = $value"
  }
}

class FieldDefExpr {
  Str field
  Str type
  Str constra

  new make(Str f, Str t, Str c := "") {
    field = f
    type = t
    constra = c
  }

  override Str toStr() {
    "$field $type $constra"
  }
}

class SelectStmt : Stmt {
  Str[] fields := [,]
  Str[] tables := [,]
  BinaryExpr? cond

  override Str toStr() {
    fieldStr := fields.join(",")
    tableStr := tables.join(",")
    s := "select $fieldStr from $tableStr"
    if (cond != null) {
      s += " where " + cond
    }
    return s
  }
}

class InsertStmt : Stmt {
  Str[] fields := [,]
  Str table := ""
  Obj[] values := [,]

  override Str toStr() {
    fieldStr := fields.join(",")
    valueStr := values.join(",")
    return "insert into $table($fieldStr) values ($valueStr)"
  }
}

class UpdateStmt : Stmt {
  AssignExpr[] sets := [,]
  Str table := ""
  Expr? cond

  override Str toStr() {
    fieldStr := sets.join(",")
    s := "update $table set $fieldStr"
    if (cond != null) {
      s += " where " + cond
    }
    return s
  }
}

class DeleteStmt : Stmt {
  Str table := ""
  Expr? cond

  override Str toStr() {
    s := "delete from $table"
    if (cond != null) {
      s += " where " + cond
    }
    return s
  }
}

class CreateStmt : Stmt {
  Str table := ""
  FieldDefExpr[] fields := [,]
  Str key := "id"
  Str type := "table"

  override Str toStr() {
    fieldStr := fields.join(",")
    return "create $type $table ($fieldStr, primary key ($key))"
  }
}

class DropStmt : Stmt {
  Str table := ""
  Str type := "table"

  override Str toStr() {
    "drop $type $table"
  }
}

class TransStmt : Stmt {
  TransState state := TransState.begin
  Int? transId

  override Str toStr() {
    str := "$state transaction"
    if (transId == null) {
      return str
    }
    return "$str $transId"
  }
}

