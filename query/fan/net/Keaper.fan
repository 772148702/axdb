//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent

const class Keeper : Actor {
  static const Str key := "axdb.KeeperActor."

  new make() : super(ActorPool{maxThreads=1}) {
    sendLater(1sec, null)
  }

  protected override Obj? receive(Obj? msg) {
    try {
      sendLater(5sec, null)

      if (ServState.cur.self == null) {
        echo("wait init")
        return null
      }

      state := ServState.cur.state
      if (state == State.leader) {
        echo("sendKeepAlive")
        sendKeepAlive
        return null
      } else if (state == State.follower) {
        Str countKey := key + "count"
        Int count := locals.get(countKey, -1)
        locals[countKey] = ++count
        if (count == 0 || count > 5) {
          locals[countKey] = 0
          echo("updateList")
          try
            updateList
          catch (Err e) { e.trace }
        }
      }

      if (Duration.nowTicks - ServState.aliveTime.val > 15sec.ticks) {
        echo("requestVote")
        requestVote
        return null
      }
    } catch (Err e) {
      e.trace
    }

    return null
  }

  private Void updateList() {
    leader := ServState.cur.leader
    if (leader == null) return

    client := AsyncClient(Action#, leader)
    Str? str := client->list()->get
    if (str == null) return
    list := str.split(',').map { it.toUri }
    list.remove(ServState.cur.self)
    list.add(leader)

    ServState.cur.change { it.list = list }
  }

  private Void sendKeepAlive() {
    Uri[] list := ServState.cur.list
    Uri self := ServState.cur.self

    list.each {
      client := AsyncClient(Action#, it)
      client->keepAlive(ServState.cur.term, ServState.cur.logId, self)
    }
  }

  private Void sendVoteResult(Bool success) {
    if (success) {
      sendKeepAlive
    }

    ServState.cur.change {
      it.state = success ? State.leader : State.follower
      it.leader = it.self
    }
  }

  private Void requestVote() {
    ServState.cur.change {
      it.term = it.term+1
      it.state = State.candidate
    }

    Uri[] list := ServState.cur.list
    futures := Future[,]
    list.each {
      client := AsyncClient(Action#, it)
      f := client->prepareVote(ServState.cur.term, ServState.cur.logId)
      futures.add(f)
    }

    count := 0
    for (i:=0; i<futures.size; ++i) {
      if (ServState.cur.state != State.candidate) break
      Str? res := futures[i].get
      if (res == null) continue

      if (res == "true")  {
        count++
        echo("vote count: $count")
        if (count+1 > (list.size+1)/2) {
          sendVoteResult(true)
          return
        }
      } else {
        sendVoteResult(false)
        return
      }
    }
  }
}