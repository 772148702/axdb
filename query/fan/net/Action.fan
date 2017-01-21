//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent
using web

class Action : Weblet {
  static const Keeper keeper := Keeper()

  override Obj? trap(Str name, Obj?[]? args := null) {
    echo("receive: $req.uri")
    return super.trap(name, args)
  }

  Bool init() {
    absUri := req.absUri
    ServState.cur.change {
      self = `${absUri.scheme}://${absUri.auth}`
      leader = self
      state = State.leader
    }
    return true
  }

  Bool? add(Uri uri) {
    cur := ServState.cur
    if (cur.state != State.leader) {
      res.redirect(cur.leader)
      return null
    }
    cur.change { it.list = it.list.dup.add(uri) }
    return true
  }

  Bool? remove(Uri uri) {
    cur := ServState.cur
    if (cur.state != State.leader) {
      res.redirect(cur.leader)
      return null
    }
    cur.change { it.list = it.list.dup { it.remove(uri) } }
    return true
  }

  Str list() {
    res.statusCode = 200
    res.headers["Content-Type"] = "text/html; charset=utf-8"
    return ServState.cur.list.join(",")
  }

  Void index() {
    res.statusCode = 200
    res.headers["Content-Type"] = "text/html; charset=utf-8"
    cur := ServState.cur
    out := res.out
    out.html.body

    out.p.w("self: $cur.self").pEnd
    out.p.w("leader: $cur.leader").pEnd
    out.p.w("state: $cur.state").pEnd
    out.p.w("term: $cur.term").pEnd
    out.p.w("logId: $cur.logId").pEnd

    cur.list.each {
      out.w("<p>- $it</p>")
    }
    out.bodyEnd.htmlEnd
  }

  Bool prepareVote(Int term, Int logId) {
    cur := ServState.cur

    if (cur.logId > logId) {
      return false
    }

    if (cur.state == State.candidate) {
      if (term <= cur.term) {
        return false
      }
    }

    cur.change { it.state = State.follower; it.term = term }
    return true
  }

  Bool keepAlive(Int term, Int logId, Uri src) {
    cur := ServState.cur
    if (term < cur.term) {
      return false
    }

    ServState.aliveTime.val = Duration.nowTicks
    absUri := req.absUri
    ServState.cur.change {
       it.leader = src; self = `${absUri.scheme}://${absUri.auth}`
       it.state = State.follower; it.term = term
    }
    return true
  }
}

const class ActionMod : RefMod {
  new make() : super.make(Action#) {}
}

