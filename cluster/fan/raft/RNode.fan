//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent
using axdbStore

enum class Role {
  leader, follower, candidate
}

@Serializable
const class NodeSate {
  const Int aliveTime
  const Int commitLog
  const Int lastApplied
  const Int lastLog
  const Int lastLogTerm
  const Int term

  const Role state

  const Uri:Int members
  const Uri? leader
  const Uri? self

  const Str name

  Void eachOthers(|Uri| f) {
    members.each |v,k| {
      if (k != self) {
        f(k)
      }
    }
  }

  override Str toStr() {
    return StrBuf() { out.writeObj(this) }.toStr
  }

  new make(|This| f) { f(this) }
}

@Serializable
const class AppendEntry {
  const Int term
  const Int leaderCommit
  const Uri leaderId

  const Int prevLogIndex
  const Int prevLogTerm

  const LogEntry[] logs

  new make(|This| f) { f(this) }

  override Str toStr() {
    return StrBuf() { out.writeObj(this) }.toStr
  }

  static new fromStr(Str str) {
    return str.in.readObj
  }
}
@Serializable { simple = true }
const class ResResult {
  const Bool success
  const Int term
  const Int lastLog

  new make(Bool success, Int term, Int lastLog) {
    this.success = success
    this.term = term
    this.lastLog = lastLog
  }

  override Str toStr() {
    "$success,$term,$lastLog"
  }

  static new fromStr(Str str) {
    ps := str.split(',')
    return ResResult(ps[0].toBool, ps[1].toInt, ps[2].toInt)
  }
}

class RNode {
  RNodeActor? actor

  Int aliveTime := Duration.nowTicks
  Int commitLog := -1
  Int lastApplied := -1
  Int lastLog := -1
  Int lastLogTerm := -1

  Int term := 0

  Uri votedFor := ``

  Role state := Role.follower

  ** uri to matchIndex
  Uri:Int members := [:]

  Uri? leader
  Uri? self

  Str name := ""

  RLogFile? logFile
  RStoreMachine? store
  Buf? persistBuf

  static const Log log := RNode#.pod.log

  override Str toStr() {
    "term:$term, lastLog:$lastLog, commitLog:$commitLog, members:$members, leader:$leader"
  }

  LogEntry? getLog(Int i) { logFile.get(i) }

  NodeSate nodeSate() {
    NodeSate {
      it.aliveTime = this.aliveTime
      it.commitLog = this.commitLog
      it.lastApplied = this.lastApplied
      it.lastLog = this.lastLog
      it.lastLogTerm = this.lastLogTerm
      it.term = this.term
      it.state = this.state
      it.members = this.members
      it.leader = this.leader
      it.self = this.self
      it.name = this.name
    }
  }

  Void setMatchIndex(Uri id, Int pos) {
    members[id] = pos
  }

  Bool changeRole(Role role) {
    this.state = role
    Uri? leader := null
    if (role == Role.leader) {
      leader = self
      echo("======leader: $self=====")
    }
    else if (role == Role.candidate) {
      this.term++
      this.votedFor = this.self
    }
    this.leader = leader
    save
    return true
  }

  private Int checkCode() {
    Int c := 0
    c = c * 31 + commitLog
    c = c * 31 + lastApplied
    c = c * 31 + term
    c = c * 31 + votedFor.hash

    members.each |v,k| {
      c = c * 31 + k.hash
    }
    return c
  }

  private Void save() {
    persistBuf.seek(0)
    out := persistBuf.out
    out.writeI8(commitLog)
    out.writeI8(lastApplied)
    out.writeI8(term)
    out.writeUtf(votedFor.toStr)

    out.writeI8(members.size)
    members.each |v,k| {
      out.writeUtf(k.toStr)
    }

    out.writeI8(checkCode)
    out.flush
    log.debug("save state")

    persistBuf.sync
  }

  private Void close() {
    save
    persistBuf.close
    persistBuf = null

    logFile.close
    logFile = null
  }

  private Void read() {
    in := persistBuf.in
    commitLog = in.readS8
    lastApplied = in.readS8
    term = in.readS8
    votedFor = in.readUtf.toUri
    msize := in.readS8
    msize.times {
      members[in.readUtf.toUri] = -1
    }

    code := in.readS8
    if (code != checkCode) {
      throw Err("check code error")
    }

    lastLog = logFile.count-1
    lastLogTerm = logFile.get(lastLog).term
    echo(this)
  }

  StoreClient getStore() {
    store.store
  }

  Bool init(File path, Str name, Uri self, Bool isLeader, StoreClient store) {
    this.name = name
    this.self = self
    this.members = [self:-1]

    if (isLeader) {
      this.leader = self
      state = Role.leader
    }

    this.store = RStoreMachine(store)
    this.logFile = StoreLogFile(path, "${name}_log")

    persistFile := (path + `${name}_state`)
    exists := persistFile.exists
    this.persistBuf = persistFile.open
    if (exists) {
      read
    } else {
      save
    }
    log.debug("init $name, $self, $isLeader")
    return true
  }

  private Void writeLog(LogEntry logEntry) {
    log.debug("add $logEntry")
    //Err("add write").trace
    logFile.add(logEntry)
    lastLog = logEntry.id
    lastLogTerm = logEntry.term
  }

  //add log from client
  LogEntry? addNewLog(Str log, Int type) {
    logId := lastLog+1
    logEntry := LogEntry(term, logId, log, type)
    writeLog(logEntry)
    this.aliveTime = Duration.nowTicks
    return logEntry
  }

  //pull result add log
  Bool addLogEntry(Int prevLogIndex, Int prevLogTerm, LogEntry log) {
    if (prevLogIndex != this.lastLog) {
      echo("$prevLogIndex != $this.lastLog")
      return false
    }
    if (this.lastLogTerm != prevLogTerm) {
      echo("$this.lastLogTerm != $prevLogTerm")
      return false
    }

    writeLog(log)
    return true
  }

  Future? commit(Int logId) {
    Future? res := null
    if (logId > this.lastLog) return res
    if (this.commitLog == logId) return res

    i := this.commitLog+1
    this.commitLog = logId

    logFile.flush

    log.debug("commit: ${i-1} => $logId")
    for (; i<=logId; ++i) {
      LogEntry? e := getLog(i)
      if (e == null) {
        echo("$i at $logFile")
      }
      if (e.type == 0) {
        res = store.apply(e)
      }
      else if (e.type == 1) {
        list := e.log.split(',').map { it.toUri }
        map := [:]
        list.each {
          map[it] = members.get(it, 0)
        }
        members = map
        echo("change members: $members")
      }
    }

    save
    return res
  }

  LogEntry onPull(Int term, Int prevLogIndex, Int prevLogTerm) {
    if (state != Role.leader) {
      return LogEntry(this.term, -2, "leader err", -1)
    }
    if (term != this.term) {
      return LogEntry(this.term, -2, "term err", -1)
    }

    if (prevLogIndex != -1) {
      entry := getLog(prevLogIndex)
      if (entry == null) {
        return LogEntry(this.term, -1, "prevLogIndex err", -1)
      }
      if (entry.term != prevLogTerm) {
        return LogEntry(this.term, -1, "prevLogTerm err", -1)
      }
    }

    next := getLog(prevLogIndex+1)
    if (next == null) {
      return LogEntry(this.term, -3, "empty", -1)
    }
    return next
  }

  Bool removeFrom(Int id) {
    if (id <= commitLog) return false
    ok := logFile.removeFrom(id)
    if (!ok) return false

    entry := getLog(id-1)
    if (entry == null) {
      lastLog = -1
      lastLogTerm = -1
    } else {
      lastLog = entry.id
      lastLogTerm = entry.id
    }
    return true
  }

  ResResult onVote(Uri candidate, Int term, Int lastLogIndex, Int lastLogTerm) {
    if (this.lastLog > lastLogIndex) {
      return ResResult(false, this.term, this.lastLog)
    }

    if (this.lastLogTerm > lastLogTerm) {
      return ResResult(false, this.term, this.lastLog)
    }

    if (this.term > term) {
      return ResResult(false, this.term, this.lastLog)
    }

    if (this.term == term && this.votedFor.toStr.size > 0) {
      return ResResult(false, this.term, this.lastLog)
    }

    echo("vote for $candidate, $this.term -> $term")

    this.state = Role.follower
    this.term = term
    this.votedFor = candidate
    save
    return ResResult(true, this.term, this.lastLog)
  }

  ResResult onReplicate(AppendEntry entry) {
    if (entry.term < this.term) {
      return ResResult(false, this.term, this.lastLog)
    }
    if (entry.logs.size > 0) {
      log.debug("$entry")
    }

    this.aliveTime = Duration.nowTicks
    this.leader = entry.leaderId
    this.state = Role.follower
    this.term = entry.term

    //echo("$entry")
    if (entry.prevLogIndex != -1) {
      if (entry.prevLogIndex > lastLog) {
        actor.sendPull
        return ResResult(false, this.term, this.lastLog)
      }
      preEntry := logFile.get(entry.prevLogIndex)
      if (preEntry == null || preEntry.term != entry.prevLogTerm) {
        actor.sendPull
        return ResResult(false, this.term, this.lastLog)
      }

      removeFrom(entry.prevLogIndex+1)
    }

    entry.logs.each {
      log := it
      writeLog(log)
    }

    commit(entry.leaderCommit)

    return ResResult(true, this.term, this.lastLog)
  }
}

