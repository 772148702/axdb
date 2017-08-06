//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent
using web
using axdbStore
using axdbQuery

class RServ : Weblet {
  private Str name
  private static const StoreMap nodeMap := StoreMap()
  private RNodeActor? nodeActor() { nodeMap[name] }

  new make(Str? name) {
    if (name == null) {
      name = "default"
    }
    this.name = name
  }

  Bool init(Bool isLeader) {
    absUri := req.absUri
    uri := `${absUri.scheme}://${absUri.auth}/$name`

    RNodeActor? tnode := nodeMap[name]
    if (tnode == null) {
      tnode = RNodeActor()
      nodeMap[name] = tnode
    }

    store := StoreClient(`./raftStore/`.toFile, name)
    res := tnode.init(`./raftState/`.toFile, name, uri, isLeader, store).get
    return res
  }

  ResResult onVote(Uri candidate, Int term, Int lastLogIndex, Int lastLogTerm) {
    nodeActor.onVote(candidate, term, lastLogIndex, lastLogTerm)
  }

  private Bool sendLog(RNodeActor actor, LogEntry entry) {
    LogEntry? pre := actor.getLog(entry.id-1)
    NodeSate nodeState := actor.nodeSate

    if (pre == null) {
      pre = LogEntry(-1, -1, "")
    }

    ae := AppendEntry {
      term = nodeState.term
      leaderCommit = nodeState.commitLog
      leaderId = nodeState.self
      prevLogIndex = pre.id
      prevLogTerm = pre.term
      logs = [entry]
    }

    futures := Uri:Future[:]
    nodeState.eachOthers {
      client := nodeActor.createClient(it)
      f := client->send_onReplicate(ae)
      futures[it] = f
    }

    return RKeeper.waitMajority(futures, actor)
  }

  Obj?[]? addNewLog(Str log, Int type:=0) {
    node := nodeActor
    NodeSate nodeState := node.nodeSate
    if (nodeState.state != Role.leader) {
      res.redirect(nodeState.leader)
      echo("redirect ${nodeState.leader}")
      return null
    }

    LogEntry? logEntry := node.addNewLog(log, type)
    if (logEntry == null) {
      return [,]
    }

    ok := sendLog(node, logEntry)
    if (!ok) {
      echo("send log fail")
      ok = sendLog(node, logEntry)
    }
    if (ok) {
      Future? res := node.commit(logEntry.id)
      if (res == null) {
        return [,]
      }
      return res.get
    }
    return [,]
  }

  ResResult onReplicate(AppendEntry entry) {
    actor := nodeActor
    if (actor == null) {
      init(false)
      actor = nodeActor
    }
    return actor.onReplicate(entry)
  }

  ResResult onKeepAlive(AppendEntry entry) {
    onReplicate(entry)
  }

  LogEntry onPull(Int term, Int prevLogIndex, Int prevLogTerm) {
    nodeActor.onPull(term, prevLogIndex, prevLogTerm)
  }

  private Engine engine() {
    Actor.locals.getOrAdd("axdb.engine") |->Engine| {
      Engine.makeStore(nodeActor.getStore)
    }
  }

  Str exeSql(Str sql) {
    Obj?[]? res
    pos := sql.index(":")
    psql := sql[pos..-1]
    if (psql.lower.startsWith(":select") ) {
      res = engine.exeSql(sql)
    } else {
      res = addNewLog(sql)
    }
    return StrBuf() { out.writeObj(res) }.toStr
  }

  Str list() {
    NodeSate cur := nodeActor.nodeSate
    return StrBuf() { out.writeObj(cur.members) }.toStr
  }

  Void index() {
    res.statusCode = 200
    res.headers["Content-Type"] = "text/html; charset=utf-8"
    out := res.out
    out.html.body

    node := nodeActor
    if (node != null) {
      NodeSate cur := node.nodeSate
      out.p.w("self: $cur.self").pEnd
      out.p.w("leader: $cur.leader").pEnd
      out.p.w("state: $cur.state").pEnd
      out.p.w("term: $cur.term").pEnd
      out.p.w("commitLog: $cur.commitLog").pEnd
      out.p.w("lastLog: $cur.lastLog").pEnd

      cur.members.each |v,k| {
        out.w("<p>$k: $v</p>")
      }
      out.p.w("$cur").pEnd
    } else {
      out.p.w("unint").pEnd
    }
    out.bodyEnd.htmlEnd
  }
}

const class ActionMod : RefMod {
  new make() : super.make(RServ#) {}
}

