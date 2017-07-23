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

  const Uri[] members
  const Uri? leader
  const Uri? self

  const Str name
  const Bool voteEnable

  Void eachOthers(|Uri| f) {
    members.each {
      if (it != self) {
        f(it)
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

  Uri? votedFor

  Role state := Role.follower

  Uri[] members := [,]
  Uri? leader
  Uri? self

  Str name := ""
  Bool voteEnable := true

  RLogFile? logFile

  RStore? store

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
      it.voteEnable = this.voteEnable
    }
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
    return true
  }

  Bool init(Str name, Uri self, Bool isLeader, StoreClient store) {
    this.name = name
    this.self = self
    this.members = [self]

    if (isLeader) {
      this.leader = self
      state = Role.leader
    }

    this.store = RStore(store)
    this.logFile = StoreLogFile(`./raftLog`.toFile, name)

    echo("init $name, $self, $isLeader")
    return true
  }

  private Void writeLog(LogEntry log) {
    logFile.add(log)
    lastLog = log.id
    lastLogTerm = log.term
    echo("add $log.id, $logFile")
    if (log.type == 1) {
      voteEnable = false
    }
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

  Void commit(Int logId) {
    if (logId > this.lastLog) return
    if (this.commitLog == logId) return

    i := this.commitLog+1
    this.commitLog = logId

    echo("commit: ${i-1} => $logId")
    for (; i<=logId; ++i) {
      LogEntry? e := getLog(i)
      if (e == null) {
        echo("$i at $logFile")
      }
      if (e.type == 1) {
        members = e.log.split(',').map { it.toUri }
        voteEnable = true
        echo("change members: $members")
      }
    }
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

    if (this.term == term && this.votedFor != null) {
      return ResResult(false, this.term, this.lastLog)
    }

    echo("vote for $candidate, $this.term -> $term")

    this.state = Role.follower
    this.term = term
    this.votedFor = candidate
    return ResResult(true, this.term, this.lastLog)
  }

  ResResult onReplicate(AppendEntry entry) {
    if (entry.term < this.term) {
      return ResResult(false, this.term, this.lastLog)
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

