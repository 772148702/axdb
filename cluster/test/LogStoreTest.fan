

class LogStoreTest : Test {
  File path := File(`data/`)
  Str name := "data"

  override Void setup() {
    path.delete
  }

  Void test() {
    logFile := StoreLogFile(path, name)
    logFile.add(LogEntry(0, 0, "text0"))
    logFile.add(LogEntry(0, 1, "text1"))
    logFile.add(LogEntry(0, 2, "text2"))

    verifyEq(logFile.get(0).log, "text0")
    verifyEq(logFile.get(1).log, "text1")
    verifyEq(logFile.get(2).log, "text2")

    verifyEq(logFile.get(-1), null)
    verifyEq(logFile.get(3), null)

    logFile.removeFrom(1)
    verifyEq(logFile.get(0).log, "text0")
    verifyEq(logFile.get(1), null)

    logFile.removeFrom(3)
  }

  Void testRemove() {
    logFile := StoreLogFile(path, name)
    logFile.add(LogEntry(0, 0, "text0"))
    logFile.add(LogEntry(0, 1, "text1"))
    logFile.add(LogEntry(0, 2, "text2"))

    logFile.removeFrom(1)
    logFile.removeFrom(3)
    verifyEq(logFile.get(0).log, "text0")
    verifyEq(logFile.get(1), null)
  }
}