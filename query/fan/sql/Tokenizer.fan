//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

enum class Token {
  keyword,
  identifier,
  literal,
  symbol,
  paren,//( or )
  semicolon,//;
  other,
  eof
}

class TokenVal {
  Token kind
  Obj? val

  new make(Token k, Obj? val := null) {
    kind = k
    this.val = val
  }

  override Str toStr() {
    "$kind: $val"
  }
}

internal class Tokenizer {
  Str src
  private Int pos := -1
  private Int cur

  private [Str:Int] keywords := [
      "select" : 1,
      "from" : 1,
      "where" : 1,
      "insert" : 2,
      "into" : 3,
      "delete" : 2,
      "create" : 2,
      "table" : 3,
      "drop" : 2,
      "group" : 2,
      "order" : 2,
      "by" : 3,
      "and" : 4,
      "or" : 4,
      "desc" : 3,
      "asc" : 3,
      "not" : 1,
      "null" : 1,
      "values" : 1,
      "update" : 1,
      "set" : 1,
      "alert":2,
      "as":1,
      "index":1,
      "between":1,
      "in":1,
      "having":1,
      "database":1,
      "like":1,
      "top":1,
      "limit":1,
      "primary":1,
      "key":2,
      "begin":2,
      "commit":2,
      "rollback":2,
      "transaction" : 2,
    ]

  new make(Str in) {
    src = in
    consume
  }

  Void rest() { pos = -1; consume }

  private Void consume() {
    ++pos
    if (pos < src.size) {
      cur = src[pos]
    }
    else cur = -1
  }

  private Int peek() {
    p := pos + 1
    if (p < src.size) {
      return src[p]
    }
    return -1
  }

  TokenVal next() {
    while (true) {
      if (cur.isSpace) {
        skipSpace
      }
      if (cur == -1) {
        return TokenVal(Token.eof)
      }
      else if (cur.isDigit) {
        return getNumber
      }
      else if (cur == '\'') {
        return getString
      }
      else if (cur == '`') {
        return getEscName
      }
      else if (cur.isAlpha) {
        return getIdentifier
      }
      else if (cur == '(' || cur == ')') {
        str := cur.toChar
        consume
        return TokenVal(Token.paren, str)
      }
      else if (cur == ';') {
        consume
        return TokenVal(Token.semicolon, ";")
      }
      else if (isSymbol) {
        return getSymbol
      }
      else {
        echo("ignore $cur.toChar")
        consume
      }
    }

    return TokenVal(Token.eof)
  }

  private Void skipSpace() {
    while (cur.isSpace) consume
  }

  private Bool isSymbol() {
    switch (cur) {
    case '+':
    case '-':
    case '*':
    case '/':
    case '%':
    case '=':
    case '!':
    case '<':
    case '>':
    case ',':
      return true
    default:
      return false
    }
  }

  private TokenVal getSymbol() {
    sb := StrBuf()
    sb.addChar(cur)
    consume
    while (isSymbol) {
        sb.addChar(cur)
        consume
    }
    return TokenVal(Token.symbol, sb.toStr)
  }

  private TokenVal getNumber() {
    sb := StrBuf()
    isFloat := false
    while (cur.isDigit || cur == '.') {
      if (cur == '.') isFloat = true
      sb.addChar(cur)
      consume
    }
    if (isFloat) {
      val := sb.toStr.toFloat
      return TokenVal(Token.literal, val)
    }
    val := sb.toStr.toInt
    return TokenVal(Token.literal, val)
  }

  private TokenVal getString() {
    sb := StrBuf()
    consume
    while (true) {
      if (cur == -1) break
      else if (cur == '\'') {
        //escape for double '
        if (peek == '\'') {
          sb.addChar('\'')
          consume
          consume
          continue
        }
        consume
        break
      }
      /*
      else if (cur == '\\' && peek == '\'') {
        sb.addChar('\'')
        consume
        consume
        continue
      }
      */
      sb.addChar(cur)
      consume
    }
    return TokenVal(Token.literal, sb.toStr)
  }

  private Str getNameStr() {
    sb := StrBuf()
    while (cur.isAlphaNum || cur == '_') {
      sb.addChar(cur)
      consume
    }
    return sb.toStr
  }

  private TokenVal getIdentifier() {
    str := getNameStr
    lower := str.lower
    if (keywords.containsKey(lower)) {
      str = lower
      if (str == "insert") {
        skipSpace
        str2 := getNameStr.lower
        if (str2 != "into") {
          throw ParseErr("expected 'insert into'")
        }
      }
      else if (str == "delete") {
        skipSpace
        str2 := getNameStr.lower
        if (str2 != "from") {
          throw ParseErr("expected 'delete from'")
        }
      }
      return TokenVal(Token.keyword, str)
    }
    return TokenVal(Token.identifier, str)
  }

  private TokenVal getEscName() {
    consume
    sb := StrBuf()
    while (cur != '`' && cur != -1) {
      sb.addChar(cur)
      consume
    }
    return TokenVal(Token.identifier, sb.toStr)
  }
}