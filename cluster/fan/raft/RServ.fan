//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent
using web

class RServ : Weblet {
  private Str name
  private RStateActor node() { RStateActor.map[name] }

  new make(Str name) {
    this.name = name
  }

  Bool init() {
    absUri := req.absUri
    uri := `${absUri.scheme}://${absUri.auth}/$name`

    actor := RStateActor()
    RStateActor.map[name] = actor
    res := actor->send_init(name, uri)->get
    return res
  }

  Bool onVote(Uri candidate, Int term, Int lastLogIndex, Int lastLogTerm) {
    node->send_onVote(candidate, term, lastLogIndex, lastLogTerm)->get
  }

  static private Obj? request(Int curLogId, RStateActor node, NodeSate nodeState) {
    logId := curLogId
    Obj? res
    while (true) {
      LogEntry entry := node->getLog(logId)
      LogEntry pre := node->getLog(logId-1)
      res = node->onReplicate(nodeState.term, nodeState.commitLog, nodeState.leader, pre.id, pre.term,
        entry.term, entry.id, entry.log)->get
      if (res != "false") {
        break
      }
      --logId
    }

    while (logId < curLogId) {
      LogEntry entry := node->getLog(logId)
      LogEntry pre := node->getLog(--logId)
      res = node->onReplicate(nodeState.term, nodeState.commitLog, nodeState.leader, pre.id, pre.term,
        entry.term, entry.id, entry.log)->get
    }
    return true
  }

  private Bool sendLog(Int logId, Str log) {
    NodeSate nodeState := node->send_nodeState
    Uri[] list := nodeState.list
    futures := Future[,]
    pool := ActorPool()

    list.each {
      actor := Actor(pool) |RStateActor msg->Obj?| {
        request(logId, node, nodeState)
      }
      f := actor->send(node)->get
      futures.add(f)
    }

    while (true) {
      count := 0
      for (i:=0; i<futures.size; ++i) {
        if (!futures[i].state.isComplete) {
          continue
        }
        Bool? res := futures[i].get
        if (res == null) continue
        if (res == true)  {
          count++
          echo("vote count: $count")
          if (count+1 > (list.size+1)/2) {
            return true
          }
        }
      }
    }
    return false
  }

  Bool? addLog(Str log) {
    Int logId := node->send_addLog(log)->get
    if (logId  == -1) {
      NodeSate nodeState := node->send_nodeSate
      if (nodeState.state != RState.leader) {
        res.redirect(nodeState.leader)
        return null
      }
      return false
    }

    res := sendLog(logId, log)
    if (res) {
      node->send_commit(logId)
    }
    return res
  }

  Bool onReplicate(Int term, Int leaderCommit, Uri leaderId, Int prevLogIndex, Int prevLogTerm
    , Int logTerm, Int logId, Str log) {
    node->send_onReplicate(term, leaderCommit, leaderId, prevLogIndex, prevLogTerm,
      logTerm, logId, log)->get
  }

  Void index() {
    res.statusCode = 200
    res.headers["Content-Type"] = "text/html; charset=utf-8"
    out := res.out
    out.html.body

    NodeSate cur := node->send_nodeSate
    out.p.w("self: $cur.self").pEnd
    out.p.w("leader: $cur.leader").pEnd
    out.p.w("state: $cur.state").pEnd
    out.p.w("term: $cur.term").pEnd
    out.p.w("commitLog: $cur.commitLog").pEnd
    out.p.w("lastLog: $cur.lastLog").pEnd

    cur.list.each {
      out.w("<p>- $it</p>")
    }
    out.bodyEnd.htmlEnd
  }
}

const class ActionMod : RefMod {
  new make() : super.make(RServ#) {}
}

