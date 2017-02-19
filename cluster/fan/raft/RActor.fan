//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent
using web

const class RStateActor : Actor {
  static const Str key := "axdb.RStateActor."
  static const StoreMap map := StoreMap()

  private const RKeeperActor keeperActor

  new make() : super(ActorPool{maxThreads=1}) {
    keeperActor = RKeeperActor(this)
  }

  protected override Obj? receive(Obj? msg) {
    Obj?[]? arg := msg
    Str name := arg[0]
    Obj?[]? args := arg[1]

    echo("receive $msg")

    try {
      node := locals.getOrAdd(key) |->RNode| { RNode() }
      return node.trap(name, args)
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

internal const class RKeeperActor : Actor {
  const RStateActor stateActor

  new make(RStateActor stateActor) : super(ActorPool{maxThreads=1}) {
    this.stateActor = stateActor
    sendLater(1sec, null)
  }

  protected override Obj? receive(Obj? msg) {
    sendLater(1sec, null)
    try {
      keeper := RKeeper(stateActor)
      keeper.check
    } catch (Err e) {
      e.trace
      throw e
    }
    return null
  }
}

**
** singleton map
**
const class StoreMap
{
  private const Actor actor := Actor(ActorPool{maxThreads=1}) |Obj?[] arg->Obj?| {return receive(arg)}

  private Obj? receive(Obj?[] arg)
  {
    Str op := arg[0]
    switch(op)
    {
      case "get":
        return Actor.locals[arg[1]]
      case "set":
        Actor.locals[arg[1]] = arg[2]
      case "remove":
        return Actor.locals.remove(arg[1])
      case "clear":
        Actor.locals.clear
      case "list":
        return Actor.locals.vals
      default:
        throw Err("unreachable code")
    }
    return null;
  }

  @Operator
  Obj? get(Str key) { actor.send(["get",key].toImmutable).get }

  Obj? list() { actor.send(["list"].toImmutable).get }

  @Operator
  Void set(Str key,Obj val) { actor.send(["set",key,val].toImmutable) }

  Void remove(Str key) { actor.send(["remove",key].toImmutable) }

  Void clear() { actor.send(["clear"].toImmutable) }
}

