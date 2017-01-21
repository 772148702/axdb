//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent
using web

enum class State {
  leader, follower, candidate
}

const class ServState {
  private static const AtomicRef curRef := AtomicRef(ServState{})
  static ServState cur() { ServState.curRef.val }

  static const AtomicInt aliveTime := AtomicInt(Duration.nowTicks)
  static const AtomicInt curLogId := AtomicInt(0)

  Int logId() { curLogId.val }

  const State state := State.follower
  const Uri[] list := [,]
  const Int term := 0
  const Uri? leader
  const Uri? self

  new make(|This| f) { f(this) }

  new makeDup(ServState other, |This| f) {
    this.state = other.state
    this.list = other.list
    this.term = other.term
    //this.logId = other.logId
    this.leader = other.leader
    this.self = other.self
    f(this)
  }

  Void change(|This| f) {
    curRef.val = makeDup(this, f)
  }
}

