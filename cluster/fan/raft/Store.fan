//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent
using axdbQuery
using axdbStore

class LogPos {
  Int pos
  Int size
  new make(|This| f) { f(this) }
}

class StoreLogFile : RLogFile {
  axdbStore::LogFile logFile
  private Int count := 0

  new make(File path, Str name) {
    logFile = axdbStore::LogFile(path, name)
  }

  Void flush() {
    logFile.flush
  }

  Void close() {
    logFile.close
  }

  override Void add(LogEntry entry) {
    buf := Buf()
    buf.writeUtf(entry.log)
    buf.writeI4(buf.size)
    buf.flip
    logFile.writeBuf(buf)
    ++count
  }

  private LogEntry readLog(LogPos beginPos) {
    Buf out := Buf()
    out.seek(0)
    logFile.readBuf(beginPos.pos, out, beginPos.size)
    Str s := out.readUtf
    return LogEntry(s)
  }

  private LogPos getBeginPos(Int endPos) {
    Int sizePos := endPos - 4
    Buf out := Buf()
    logFile.readBuf(sizePos, out, 4)
    out.flip
    size := out.readS4
    beginPos := sizePos - size
    return LogPos { it.pos = beginPos; it.size = size }
  }

  private LogPos? getLogPos(Int i) {
    if (i >= count) return null
    p := count-1
    endPos := logFile.length
    LogPos? logPos
    while (p >= i) {
      logPos = getBeginPos(endPos)
      endPos = logPos.pos
      --p
    }
    return logPos
  }

  override LogEntry? get(Int i) {
    if (i >= count) return null
    p := getLogPos(i)
    return readLog(p)
  }

  override Bool removeFrom(Int i) {
    if (i >= count) return false
    p := getLogPos(i)
    return removeFrom(p.pos)
  }
}

const class RStore {
  const StoreClient store

  new make(StoreClient store) {
    this.store = store
  }

  Engine engine() {
    Actor.locals.getOrAdd("axdb.engine") |->Engine| {
      Engine.makeStore(store)
    }
  }

  Obj?[] exeSql(Str sql) {
    engine.exeSql(sql)
  }
}