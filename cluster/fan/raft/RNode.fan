//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent

enum class RState {
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

  const RState state

  const Uri[] list
  const Uri? leader
  const Uri? self

  const Str name

  new make(|This| f) { f(this) }
}

const class LogEntry {
  const Int term
  const Int id
  const Str log

  new make(Int term, Int id, Str log) {
    this.term = term
    this.id = id
    this.log = log
  }
}

class LogFile {
  LogEntry[] logs := [,]

  Void add(LogEntry entry) { logs.add(entry) }
  LogEntry get(Int i) { logs[i] }
  Void removeFrom(Int i) { logs.removeRange(i..-1) }
}

class RNode {
  Int aliveTime := Duration.nowTicks
  Int commitLog := -1
  Int lastApplied := -1
  Int lastLog := -1
  Int lastLogTerm := -1

  Int term := 0

  Uri? votedFor

  RState state := RState.follower

  Uri[] list := [,]
  Uri? leader
  Uri? self

  Str name := ""

  LogFile logFile := LogFile()

  LogEntry getLog(Int i) { logFile.get(i) }

  NodeSate nodeSate() {
    NodeSate {
      it.aliveTime = this.aliveTime
      it.commitLog = this.commitLog
      it.lastApplied = this.lastApplied
      it.lastLog = this.lastLog
      it.lastLogTerm = this.lastLogTerm
      it.term = this.term
      it.state = this.state
      it.list = this.list
      it.leader = this.leader
      it.self = this.self
      it.name = this.name
    }
  }

  Bool changeRole(RState role) {
    this.state = role
    if (role == RState.leader) {
      this.leader = self
    }
    else if (role == RState.candidate) {
      this.term++
    }
    return true
  }

  Bool init(Str name, Uri self) {
    this.name = name
    this.self = self
    return true
  }

  private Void writeLog(Int logId, Str log) {
  }

  Int addLog(Str log) {
    logId := lastLog+1
    writeLog(logId, log)
    lastLog = logId
    return logId
  }

  Void commit(Int logId) {
    if (logId > this.lastLog) return
    this.commitLog = logId
  }

  Bool onVote(Uri candidate, Int term, Int lastLogIndex, Int lastLogTerm) {
    if (this.lastLog > lastLogIndex) {
      return false
    }

    if (this.lastLogTerm > lastLogTerm) {
      return false
    }

    if (this.state == RState.candidate) {
      if (term <= this.term) {
        return false
      }
    }

    if (this.term == term && this.votedFor != null) {
      return false
    }

    this.state = RState.follower
    this.term = term
    this.votedFor = candidate
    return true
  }

  Bool appendEntries(Int term, Int leaderCommit, Uri leaderId, Int prevLogIndex, Int prevLogTerm,
    Int logTerm, Int logId, Str log) {
    if (term < this.term) {
      return false
    }

    this.aliveTime = Duration.nowTicks
    this.leader = leaderId
    this.state = RState.follower
    this.term = term

    if (prevLogIndex != -1) {
      if (prevLogIndex > lastLog) {
        return false
      }
      preEntry := logFile.get(prevLogIndex)
      if (preEntry.term != prevLogTerm) {
        return false
      }

      logFile.removeFrom(prevLogIndex)
    }

    if (logId != -1) {
      entry := LogEntry(logTerm, logId, log)
      logFile.add(entry)

      this.lastLog = logId
      this.lastLogTerm = logTerm
      this.commitLog = leaderCommit
    }

    return true
  }
}

