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

  override Int count {
    get { return logFile.userData }
    set { logFile.userData = it }
  }

  new make(File path, Str name) {
    logFile = axdbStore::LogFile(path, name)
    logFile.open
  }

  override Void flush() {
    logFile.flush
  }

  override Void close() {
    logFile.close
  }

  override Void add(LogEntry entry) {
    str := entry.toStr
    buf := Buf()
    buf.writeUtf(str)
    buf.writeI4(buf.size)
    buf.flip
    logFile.writeBuf(buf)
    ++count
  }

  private LogEntry readLog(LogPos beginPos) {
    Buf out := Buf()
    out.seek(0)
    logFile.readBuf(beginPos.pos, out, beginPos.size)
    out.flip
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
    if (i >= count || i < 0) return null
    p := getLogPos(i)
    return readLog(p)
  }

  override Bool removeFrom(Int i) {
    if (i >= count) return false
    p := getLogPos(i)
    res := logFile.removeFrom(p.pos)
    if (res) {
      count = i
    }
    return res
  }
}

const class RStoreMachine : Actor{
  const StoreClient store
  private const RNodeActor nodeActor

  new make(StoreClient store, ActorPool pool, RNodeActor nodeActor) : super.make(pool) {
    this.store = store
    this.nodeActor = nodeActor
  }

  protected override Obj? receive(Obj? msg) {
    LogEntry e := msg
    res := engine.exeSql(e.log)
    nodeActor.setLastApplied(e.id)
    return res.toImmutable
  }

  Void close() {
  }

  Future apply(LogEntry log) {
    this.send(log.toImmutable)
  }

  private Engine engine() {
    Actor.locals.getOrAdd("axdb.engine") |->Engine| {
      Engine.makeStore(store)
    }
  }
}