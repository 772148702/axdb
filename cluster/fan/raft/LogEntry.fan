//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//


@Serializable { simple = true }
const class LogEntry {
  const Int type
  const Int term
  const Int id
  const Str log

  new make(Int term, Int id, Str log, Int type:=0) {
    this.term = term
    this.id = id
    this.log = log
    this.type = type
  }

  override Str toStr() {
    "$type,$term,$id,$log"
  }

  static new fromStr(Str str) {
    vs := Util.split(str, ",", 4)
    return LogEntry(vs[1].toInt, vs[2].toInt, vs[3], vs[0].toInt)
  }
}

class LogFile {
  LogEntry[] logs := [,]

  Void add(LogEntry entry) { logs.add(entry) }

  LogEntry? get(Int i) { logs.getSafe(i) }

  Bool removeFrom(Int i) {
    if (i >= logs.size) return false
    logs.removeRange(i..-1)
    return true
  }
}


