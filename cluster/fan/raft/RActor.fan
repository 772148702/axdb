//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent
using web
using axdbStore

const class RNodeActor : Actor {
  private static const Str key := "axdb.RStateActor."

  private const RKeeper keeper

  new make() : super(ActorPool{maxThreads=1}) {
    keeper = RKeeper(this)
  }

  Void sendPull() {
    keeper.sendPull
  }

  protected override Obj? receive(Obj? msg) {
    Obj?[]? arg := msg
    Str method := arg[0]
    Obj?[]? args := arg[1]

    //echo("receive $msg")

    try {
      node := locals.getOrAdd(key) |->RNode| { RNode { actor = this } }
      return node.trap(method, args)
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

  Future init(File path, Str name, Uri self, Bool isLeader, StoreClient store) {
    this.send(["init", [path, name, self, isLeader, store].toImmutable].toImmutable)
  }

  Future close() {
    this.send(["close", [,].toImmutable].toImmutable)
  }

  NodeSate nodeSate() {
    this.send(["nodeSate", [,].toImmutable].toImmutable).get
  }

  StoreClient getStore() {
    this.send(["getStore", [,].toImmutable].toImmutable).get
  }

  Void setMatchIndex(Uri id, Int pos) {
    this.send(["setMatchIndex", [id, pos].toImmutable].toImmutable)
  }

  Future changeRole(Role role) {
    this.send(["changeRole", [role].toImmutable].toImmutable)
  }

  Bool removeFrom(Int id) {
    this.send(["removeFrom", [id].toImmutable].toImmutable).get
  }

  LogEntry onPull(Int term, Int prevLogIndex, Int prevLogTerm) {
    this.send(["onPull", [term, prevLogIndex, prevLogTerm].toImmutable].toImmutable).get
  }

  ResResult onVote(Uri candidate, Int term, Int lastLogIndex, Int lastLogTerm) {
    this.send(["onVote", [candidate, term, lastLogIndex, lastLogTerm].toImmutable].toImmutable).get
  }

  ResResult onReplicate(AppendEntry entry) {
    this.send(["onReplicate", [entry].toImmutable].toImmutable).get
  }

  LogEntry? getLog(Int i) {
    this.send(["getLog", [i].toImmutable].toImmutable).get
  }

  LogEntry? addNewLog(Str log, Int type) {
    this.send(["addNewLog", [log, type].toImmutable].toImmutable).get
  }

  Future? commit(Int logId) {
    this.send(["commit", [logId].toImmutable].toImmutable).get
  }

  Bool addLogEntry(Int prevLogIndex, Int prevLogTerm, LogEntry le) {
    this.send(["addLogEntry", [prevLogIndex, prevLogTerm, le].toImmutable].toImmutable).get
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