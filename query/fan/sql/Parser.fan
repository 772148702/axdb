//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//
using axdbStore

class Parser {
  private CmpUnit unit := CmpUnit()
  private Tokenizer tokenzier
  private TokenVal cur

  new make(Str src) {
    tokenzier = Tokenizer(src)
    cur = tokenzier.next
  }

  private Void consume() {
    cur = tokenzier.next
  }

  CmpUnit parse() {
    while (cur.kind != Token.eof) {
      if (cur.kind != Token.keyword) {
        throw ArgErr("unknow token $cur")
      }
      switch (cur.val) {
        case "select":
          unit.add(selectStmt)
        case "insert":
          unit.add(insertStmt)
        case "delete":
          unit.add(deleteStmt)
        case "update":
          unit.add(updateStmt)
        case "create":
          unit.add(createStmt)
        case "drop":
          unit.add(dropStmt)
        case "begin":
          unit.add(parseTrans(TransState.begin))
        case "commit":
          unit.add(parseTrans(TransState.commit))
        case "rooback":
          unit.add(parseTrans(TransState.abort))
        default:
          throw ArgErr("unknow token $cur")
      }

      if (cur.kind == Token.semicolon) {
        consume
        continue
      } else {
        break
      }
    }
    return unit
  }

  private Obj? getAndConsume(Token type) {
    if (cur.kind != type) {
      throw ArgErr("expected type $type but $cur")
    }
    val := cur.val
    consume
    return val
  }

  private Void consumeAs(Token type, Obj? val) {
    if (cur.kind != type) {
      throw ArgErr("expected type $type but $cur")
    }
    if (cur.val != val) {
      throw ArgErr("expected val $val but $cur")
    }
    consume
  }

  ////////////////////////////////////////////////////////////////////////
  // Expr
  ////////////////////////////////////////////////////////////////////////

  private Expr? parsePrimitive() {
    if (cur.kind == Token.literal) {
      expr := LiteralExpr(cur.val)
      consume
      return expr
    }
    else if (cur.kind == Token.identifier) {
      expr := IdExpr(cur.val)
      consume
      return expr
    }
    return null
  }

  private Expr? parseParen() {
    if (cur.kind == Token.paren && cur.val == "(") {
      consume
      expr := parseExpr
      consumeAs(Token.paren, ")")
      return expr
    }
    return parsePrimitive
  }

  private Expr? parseBinarySymbol(Str[][] symbols, Int prece) {
    if (prece >= symbols.size) {
      return parseParen
    }

    expr := parseBinarySymbol(symbols, prece+1)
    while (cur.kind == Token.symbol || cur.kind == Token.keyword) {
      if (symbols[prece].contains(cur.val)) {
        op := cur.val
        consume
        right := parseBinarySymbol(symbols, prece+1)
        //echo("$expr, $op, $right")
        expr = BinaryExpr(expr, op, right)
      } else {
        break
      }
    }
    return expr
  }

  private Expr parseExpr() {
    symbols := [
      ["or"],
      ["and"],
      ["!=", "<>", ">=", "<=", "=="],
      ["<", ">", "="],
      ["+", "-"],
      ["*", "/", "%"],
    ]
    return parseBinarySymbol(symbols, 0)
  }

  ////////////////////////////////////////////////////////////////////////
  // Statment
  ////////////////////////////////////////////////////////////////////////

  private Str[] parseNameList() {
    list := Str[,]
    while (true) {
      if (cur.kind == Token.identifier) {
        field := cur.val
        list.add(field)
        consume
      }
      else if (cur.kind == Token.symbol && cur.val == "*") {
        consume
      }

      if (cur.kind == Token.symbol && cur.val == ",") {
        consume
      } else {
        break
      }
    }
    return list
  }

  private Stmt selectStmt() {
    consume
    stmt := SelectStmt()
    stmt.fields = parseNameList

    consumeAs(Token.keyword, "from")
    stmt.tables = parseNameList
    stmt.cond = parseWhere

    return stmt
  }

  private Stmt insertStmt() {
    consume
    stmt := InsertStmt()
    stmt.table = getAndConsume(Token.identifier)

    if (cur.kind == Token.paren && cur.val == "(") {
      consume
      stmt.fields = parseNameList
      consumeAs(Token.paren, ")")
    }

    consumeAs(Token.keyword, "values")

    if (cur.kind == Token.paren && cur.val == "(") {
      consume
      while (true) {
        val := getAndConsume(Token.literal)
        stmt.values.add(val)
        if (cur.kind == Token.symbol && cur.val == ",") {
          consume
        } else {
          break
        }
      }
      consumeAs(Token.paren, ")")
    }

    return stmt
  }

  private Stmt updateStmt() {
    consume
    stmt := UpdateStmt()
    stmt.table = getAndConsume(Token.identifier)
    consumeAs(Token.keyword, "set")

    while (cur.kind == Token.identifier) {
      name := cur.val
      consume
      consumeAs(Token.symbol, "=")
      value := getAndConsume(Token.literal)
      assign := AssignExpr(name, value)
      stmt.sets.add(assign)

      if (cur.kind == Token.symbol && cur.val == ",") {
        consume
      } else {
        break
      }
    }

    stmt.cond = parseWhere
    return stmt
  }

  private Expr? parseWhere() {
    if (cur.kind == Token.keyword && cur.val == "where") {
      consume
      return parseExpr
    }
    return null
  }

  private Stmt deleteStmt() {
    consume
    stmt := DeleteStmt()

    stmt.table = getAndConsume(Token.identifier)
    stmt.cond = parseWhere

    return stmt
  }

  private FieldDefExpr parseFieldDef() {
    field := getAndConsume(Token.identifier)
    type := getAndConsume(Token.identifier)

    if (cur.kind == Token.paren && cur.val == "(") {
      consume
      while (true) {
        if (cur.kind == Token.paren && cur.val == ")") {
          consume
          break
        }
        //ignore
        consume
      }
    }

    def := FieldDefExpr(field, type)

    while (cur.kind == Token.keyword) {
      if (def.constra == "") {
        def.constra = cur.val
      } else {
        def.constra += " "+ cur.val
      }
      consume
    }
    return def
  }

  private Stmt createStmt() {
    consume
    stmt := CreateStmt()
    stmt.type = getAndConsume(Token.keyword)
    stmt.table = getAndConsume(Token.identifier)

    if (cur.kind == Token.paren && cur.val == "(") {
      consume
      while (true) {
        if (cur.kind == Token.keyword && cur.val == "primary") {
          consume
          consumeAs(Token.keyword, "key")
          consumeAs(Token.paren, "(")
          stmt.key = getAndConsume(Token.identifier)
          consumeAs(Token.paren, ")")
        }
        else {
          stmt.fields.add(parseFieldDef)
        }

        if (cur.kind == Token.symbol && cur.val == ",") {
          consume
        } else {
          break
        }
      }
      consumeAs(Token.paren, ")")
    }

    return stmt
  }

  private Stmt dropStmt() {
    consume
    stmt := DropStmt()
    stmt.type = getAndConsume(Token.keyword)
    stmt.table = getAndConsume(Token.identifier)
    return stmt
  }

  private Stmt parseTrans(TransState state) {
    consume
    if (cur.val != "transaction") {
      throw ArgErr("expected transaction")
    }
    consume

    Int? transId
    if (cur.kind == Token.literal && cur.val != null) {
      transId = cur.val as Int
    }
    trans := TransStmt { it.state = state; it.transId = transId }
    return trans
  }
}

