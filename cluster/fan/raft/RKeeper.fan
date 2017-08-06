//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent

const class RKeeper : Actor {
  private const RNodeActor nodeActor
  private const Duration keepAliveTime := 5sec

  new make(RNodeActor nodeActor, ActorPool pool) : super.make(pool) {
    this.nodeActor = nodeActor
    this.sendLater(keepAliveTime, null)
  }

  protected override Obj? receive(Obj? msg) {
     try {
      //echo("keeper actor: $msg")
      keeper := this
      if (msg == null) {
         this.sendLater(keepAliveTime, null)
         keeper.check
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

  Void sendVote() {
    mils := (0..1000).random
    this.sendLater(Duration(mils*1000_000), "vote")
  }

  private NodeSate? getNode() {
    nodeActor.nodeSate
  }

  private Void changeRole(Role role) {
    nodeActor.changeRole(role)
  }

  private Void check() {
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
       sendVote
    }
  }

  private Void vote() {
    node := getNode
    if (Duration.nowTicks - node.aliveTime < 10sec.ticks) {
     return
    }
    echo("requestVote")
    if (requestVote) {
      changeRole(Role.leader)
      client := nodeActor.createClient(node.self)
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
      client := nodeActor.createClient(it)
      client->send_onKeepAlive(ae)
    }
  }

  static Bool waitMajority(Uri:Future futures, RNodeActor nodeActor) {
    allSize := futures.size

    echo("waitMajority: $allSize")

    count := 0
    if (allSize == 0) return true

    success := false
    while (futures.size > 0) {
      success = futures.dup.any |f, k| {
        if (!f.state.isComplete) {
          return false
        }
        futures.remove(k)

        Str? str := f.get
        echo("$str")
        if (str != null) {
          res := ResResult(str)
          if (res.success) {
            count++
            echo("vote count: $count, ${count+1} > ${(allSize+1)/2}")
            if (count+1 > (allSize+1)/2) {
              return true
            }
          }
          nodeActor.setMatchIndex(k, res.lastLog)
        }
        return false
      }
      if (success) break
      Actor.sleep(5ms)
    }
    return success
  }

  private Bool requestVote() {
    changeRole(Role.candidate)

    node := getNode
    futures := Uri:Future[:]
    node.eachOthers {
      client := nodeActor.createClient(it)
      //Uri candidate, Int term, Int lastLogIndex, Int lastLogTerm
      f := client->send_onVote(node.self, node.term, node.lastLog, node.lastLogTerm)
      futures[node.self] = f
    }

    return waitMajority(futures, nodeActor)
  }
}

const class RPullActor : Actor {
  private const RNodeActor nodeActor

  new make(RNodeActor nodeActor, ActorPool pool) : super.make(pool) {
    this.nodeActor = nodeActor
  }

  protected override Obj? receive(Obj? msg) {
     try {
      if (msg == "pull") {
        this.pull
      }
    } catch (Err e) {
      e.trace
      throw e
    }
    return null
  }

  Void sendPull() {
    echo("send Pull")
    this.send("pull")
  }

  private Void pull() {
    //echo("pull *******")
    node := nodeActor.nodeSate
    client := nodeActor.createClient(node.leader)
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