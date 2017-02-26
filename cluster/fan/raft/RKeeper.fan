//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent

class RKeeper {
  RStateActor state

  new make(RStateActor state) {
    this.state = state
  }

  NodeSate? getNode() {
    state->send_nodeStae->get
  }

  private Void changeRole(RState role) {
    state->send_changeRole(role)
  }

  Void check() {
    node := getNode

    if (node.self == null) {
      echo("wait init")
      return null
    }

    if (node.state == RState.leader) {
      sendKeepAlive
      return
    }

    if (Duration.nowTicks - node.aliveTime > 15sec.ticks) {
       if (requestVote) {
         changeRole(RState.leader)
       } else {
         changeRole(RState.follower)
       }
    }
  }

  private Void sendKeepAlive() {
    node := getNode
    node.list.each {
      client := RpcClient(RServ#, it)
      client->send_onKeepAlive(node.term, node.commitLog, node.lastLog, node.self)
    }
  }

  static Bool waitMajority(Future[] futures) {
    size := futures.size
    count := 0
    while (futures.size > 0) {
      for (i:=0; i<futures.size; ++i) {
        f := futures[i]
        if (!f.state.isComplete) {
          continue
        }
        futures.removeAt(i)

        Str? res := f.get
        if (res == "true")  {
          count++
          //echo("vote count: $count")
          if (count+1 > (size+1)/2) {
            return true
          }
        }
      }
      Actor.sleep(5ms)
    }
    return false
  }

  private Bool requestVote() {
    changeRole(RState.candidate)

    node := getNode
    Uri[] list := node.list
    futures := Future[,]
    list.each {
      client := RpcClient(RServ#, it)
      f := client->send_onVote(node.term, node.lastLog)
      futures.add(f)
    }

    return waitMajority(futures)
  }

  Void pull() {
    node := getNode
    client := RpcClient(RServ#, node.leader)
    Str? res := client->onPull(node.term, node.lastLog, node.lastLogTerm)
    if (res == null || res.startsWith("-2")) {
      return
    }

    if (res == "-1") {
      entry := state->send_removeFrom(node.lastLog)
      if (entry != null) {
        state.keeperActor.send("pull")
      }
    }
  }
}