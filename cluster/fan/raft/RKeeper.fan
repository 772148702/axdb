//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent

const class RKeeper {
  private const RNodeActor nodeActor
  private const Actor keeperActor
  private const Duration keepAliveTime := 5sec

  new make(RNodeActor nodeActor) {
    this.nodeActor = nodeActor
    keeperActor = Actor(ActorPool{maxThreads=1}) |Obj? arg->Obj?| { return onKeep(arg) }
    keeperActor.sendLater(keepAliveTime, null)
  }

  private Obj? onKeep(Obj? msg) {
     try {
      //echo("keeper actor: $msg")
      keeper := this
      if (msg == null) {
         keeperActor.sendLater(keepAliveTime, null)
         keeper.check
      }
      else if (msg == "pull") {
        keeper.pull
      }
      else if (msg == "vote") {
        keeper.vote
      }
    } catch (Err e) {
      e.trace
      throw e
    }
    return null
  }

  Void sendPull() {
    echo("send Pull")
    keeperActor.send("pull")
  }

  Void sendVote() {
    mils := (0..1000).random
    keeperActor.sendLater(Duration(mils*1000_000), "vote")
  }

  private NodeSate? getNode() {
    nodeActor.nodeSate
  }

  private Void changeRole(Role role) {
    nodeActor.changeRole(role)
  }

  Void check() {
    node := getNode

    if (node.self == null) {
      echo("wait init")
      return null
    }

    if (node.state == Role.leader) {
      if (Duration.nowTicks - node.aliveTime > 5sec.ticks) {
        echo("sendKeepAlive")
        sendKeepAlive
      }
      return
    }

    if (Duration.nowTicks - node.aliveTime > 10sec.ticks) {
       if (node.voteEnable) {
         sendVote
       }
    }
  }

  Void vote() {
    node := getNode
    if (Duration.nowTicks - node.aliveTime < 10sec.ticks) {
     return
    }
    echo("requestVote")
    if (requestVote) {
      changeRole(Role.leader)
      client := RpcClient(RServ#, node.self)
      suc := client->addNewLog("take office", 2)
      if (suc != "true") {
        changeRole(Role.follower)
      }
    } else {
     changeRole(Role.follower)
    }
  }

  private Void sendKeepAlive() {
    node := getNode
    ae := AppendEntry {
      term = node.term
      leaderCommit = node.commitLog
      leaderId = node.self
      prevLogIndex = node.lastLog
      prevLogTerm = node.lastLogTerm
      logs = [,]
    }
    node.eachOthers {
      client := RpcClient(RServ#, it)
      client->send_onKeepAlive(ae)
    }
  }

  static Bool waitMajority(Uri:Future futures, RNodeActor nodeActor) {
    size := futures.size
    count := 0
    if (size == 0) return true

    while (futures.size > 0) {
      futures.dup.each |f, k| {
        if (!f.state.isComplete) {
          return
        }
        futures.remove(k)

        Str? str := f.get
        if (str != null) {
          res := ResResult(str)
          if (res.success) {
            count++
            //echo("vote count: $count")
            if (count+1 > (size+1)/2) {
              return true
            }
          }
          nodeActor.setMatchIndex(k, res.lastLog)
        }
      }
      Actor.sleep(5ms)
    }
    return false
  }

  private Bool requestVote() {
    changeRole(Role.candidate)

    node := getNode
    futures := Uri:Future[:]
    node.eachOthers {
      client := RpcClient(RServ#, it)
      //Uri candidate, Int term, Int lastLogIndex, Int lastLogTerm
      f := client->send_onVote(node.self, node.term, node.lastLog, node.lastLogTerm)
      futures[node.self] = f
    }

    return waitMajority(futures, nodeActor)
  }

  Void pull() {
    //echo("pull *******")
    node := getNode
    client := RpcClient(RServ#, node.leader)
    Str? res := client->onPull(node.term, node.lastLog, node.lastLogTerm)
    echo("pull receive $res")
    if (res == null) return

    log := LogEntry(res)
    if (log.type == -1) {
      switch (log.id) {
        case -2:
          return
        case -1:
          success := nodeActor.removeFrom(node.lastLog)
          if (success) {
            nodeActor.sendPull
          }
          return
        case -3:
          return
      }
      return
    }

    ok := nodeActor.addLogEntry(node.lastLog, node.lastLogTerm, log)
    if (!ok) {
      echo("addLogEntry err")
    }
    nodeActor.sendPull
  }
}