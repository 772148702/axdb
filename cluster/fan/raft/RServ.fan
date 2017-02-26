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

  private Bool sendLog(Int logId, Str log) {
    LogEntry entry := node->getLog(logId)
    LogEntry pre := node->getLog(logId-1)
    NodeSate nodeState := node->send_nodeSate

    Uri[] list := nodeState.list
    futures := Future[,]
    list.each {
      client := RpcClient(RServ#, it)
      f := node->onReplicate(nodeState.term, nodeState.commitLog, nodeState.leader, pre.id, pre.term,
        entry.term, entry.id, entry.log)
      futures.add(f)
    }

    return RKeeper.waitMajority(futures)
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

    ok := sendLog(logId, log)
    if (!ok) {
      ok = sendLog(logId, log)
    }
    if (ok) {
      node->send_commit(logId)
    }
    return ok
  }

  Bool onReplicate(Int term, Int leaderCommit, Uri leaderId, Int prevLogIndex, Int prevLogTerm
    , Int logTerm, Int logId, Str log) {
    node->send_onReplicate(term, leaderCommit, leaderId, prevLogIndex, prevLogTerm,
      logTerm, logId, log)->get
  }

  Str onPull(Int term, Int prevLogIndex, Int prevLogTerm) {
    node->send_onPull(term, prevLogIndex, prevLogTerm)->get
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

