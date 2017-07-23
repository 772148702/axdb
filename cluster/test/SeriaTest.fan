

class SeriaTest : Test {

  Void test1() {
    ae := AppendEntry {
      term = 1
      leaderCommit = 2
      leaderId = `1`
      prevLogIndex = 3
      prevLogTerm = 0
      logs = [ LogEntry(0, 0, "text") ]
    }
    str := ae.toStr
    echo(str)
    ae2 := AppendEntry(str)

    verifyEq(ae.logs[0].log, ae2.logs[0].log)
  }

  Void test2() {
    res := ResResult(true, 0, 1)
    str := res.toStr
    echo(str)
    res2 := ResResult(str)

    verifyEq(res.success, res2.success)
  }
}