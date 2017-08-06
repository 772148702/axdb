//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using axdbStore
using util

class Record {
  [Str:Obj]? vals
  Record[]? list
}

class Plan {
  Executor? executor
  virtual Record? next() { null }
  virtual Void reset() {}
}

class IdSearchPlan : Plan {
  Str table
  Buf key
  private Int pos

  new make(|This| f) { f(this) }

  override Record? next() {
    if (pos == 0) {
      valbuf := executor.engine.search(executor.transId, table, key)
      data := JsonInStream(valbuf.in).readJson
      ++pos
      return Record { vals = data }
    }
    return null
  }
}

class ScanPlan : Plan {
  Str table
  private BTreeIterator? itr

  new make(|This| f) { f(this) }

  override Record? next() {
    if (itr == null) {
      executor.engine.scan(executor.transId, table)
    }
    if (!itr.more) return null
    valbuf := itr.next
    data := JsonInStream(valbuf.in).readJson
    return Record { vals = data }
  }
}

class CrossJoinPlan : Plan {
  Plan outer
  Plan inner

  private Record? curO
  private Record? curI

  new make(|This| f) { f(this) }

  override Record? next() {
    curI = inner.next
    if (curI == null) {
      curO = outer.next
      if (curO == null) return null
      inner.reset
      curI = inner.next
      if (curI == null) return null
    }

    return Record { list = [curO, curI] }
  }
}

