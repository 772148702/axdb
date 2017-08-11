//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent

abstract const class BaseActor : Actor {
  protected static const Str key := "axdb.store."

  new make() : super(ActorPool{maxThreads=1}) {}

  protected override Obj? receive(Obj? msg) {
    Obj?[]? arg := msg
    Str name := arg[0]
    Obj?[]? args := arg[1]

    PageMgr.log.debug("receive $msg")

    try {
      return trap(name, args)
    } catch (Err e) {
      e.trace
      throw e
    }
  }

  override Obj? trap(Str name, Obj?[]? args := null) {
    if (name.startsWith("send_")) {
      method := name[5..-1]
      return this.send([method, args].toImmutable)
    }
    return super.trap(name, args)
  }
}

const class StoreActor : BaseActor {
  private const File path
  private const Str name

  private BlockStore store() {
    Actor.locals.getOrAdd(key+"store") |->BlockStore| {
      BlockStore(path, name)
    }
  }

  new make(File path, Str name) {
    this.path = path
    this.name = name
  }

  protected override Obj? receive(Obj? msg) {
    Obj?[]? arg := msg
    Str name := arg[0]
    Obj?[]? args := arg[1]

    PageMgr.log.debug("receive $msg")

    try {
      return store.trap(name, args)
    } catch (Err e) {
      e.trace
      throw e
    }
  }
}

const class CacheActor : BaseActor {
  const Int size

  private static const Str transIdCount := key + "transIdCount"

  private LruCache cache() {
    Actor.locals.getOrAdd(key + "cache") |->LruCache| {
      LruCache(size)
    }
  }

  private [Int:Int] transMap() {
    Actor.locals.getOrAdd(key + "transMap") |->[Int:Int]| {
      [Int:Int][:]
    }
  }

  protected override Obj? receive(Obj? msg) {
    Obj?[]? arg := msg
    Str name := arg[0]
    Obj?[]? args := arg[1]
    try {
      switch (name) {
        case "beginTrans":
          Int? id := args[0]
          if (id == null) {
            id = Actor.locals.get(transIdCount, 0)
            Actor.locals[transIdCount] = id + 1
          }
          transMap[id] = 1
          return id

        case "endTrans":
          transMap.remove(args[0])
          return null

        case "hasTrans":
          return transMap.containsKey(args[0])

        case "getCahce":
          return cache.get(args[0])

        case "setCache":
          cache.set(args[0], args[1])
          return null
        default:
      }
    } catch (Err e) {
      e.trace
      throw e
    }
    return null
  }

  new make(Int size) {
    this.size = size
  }

  Int beginTrans(Int transId) {
    this->send_beginTrans(transId)->get
  }

  Void endTrans(Int transId) {
    this->send_endTrans(transId)
  }

  Bool hasTrans(Int transId) {
    this->send_hasTrans(transId)->get
  }

  Obj? getCache(Obj key) {
    this->send_getCache(key)->get
  }

  Void setCache(Obj key, Obj? val) {
    this->send_setCache(key, val)
  }

}