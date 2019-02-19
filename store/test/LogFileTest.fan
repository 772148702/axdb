
class LogFileTest : Test {
  File path := File(`data/`)
  Str name := "data"

  override Void setup() {
    path.delete
  }

  private Void write() {
    log := LogFile(path, name)
    log.fileSize = 64
    log.open

    100.times {
      buf := Buf()
      buf.out.writeUtf("$it;")
      buf.flip
      log.writeBuf(buf)
    }

    log.dump
    echo("lastPos:$log.length")

    log.close
  }


  Void testWrite() {
    write

    log := LogFile(path, name)
    log.open

    verifyEq(log.fileSize, 64)

    100.times {
      buf := Buf()
      buf.out.writeUtf("$it,")
      buf.flip
      log.writeBuf(buf)
    }
    log.dump
    log.close
  }

  Void testTrim() {
    testWrite

    log := LogFile(path, name)
    log.open

    log.dump
    log.trim(490)
    log.dump
    verifyEq(log.fileCount, 9)

    log.close
  }

  Void testTrimWrite() {
    testTrim

    log := LogFile(path, name)
    log.open

    100.times {
      buf := Buf()
      buf.out.writeUtf("$it-")
      buf.flip
      log.writeBuf(buf)
    }
    log.dump

    log.close
  }

  Void testRead() {
    testTrim
    log := LogFile(path, name)
    log.open
    log.dump

    buf := Buf()
    n := log.readBuf(0, buf, 100)
    buf.flip
    //echo("n=$n, pos=$buf.pos,size=$buf.size")
    str := buf.readUtf
    verifyEq(str, "0,")

    log.close
  }


  private Void testSeek() {
    log := LogFile(path, name)
    log.fileSize = 64
    log.open

    buf := Buf()
    buf.size = 32
    log.writeBuf(buf)

    log.dump
    echo("lastPos:$log.length")
    verifyEq(log.length, 32)
    verifyEq(log.fileCount, 1)

    //read
    buf.seek(0)
    log.readBuf(0, buf, 32)
    log.dump
    echo("lastPos:$log.length")
    verifyEq(log.length, 32)
    verifyEq(log.fileCount, 1)

    buf.seek(0)
    buf.size = 32
    log.writeBuf(buf)
    log.dump
    echo("lastPos:$log.length")
    verifyEq(log.length, 64)
    verifyEq(log.fileCount, 1)

    buf.seek(0)
    log.writeBuf(buf)
    log.dump
    echo("lastPos:$log.length")
    verifyEq(log.length, 96)
    verifyEq(log.fileCount, 2)

    log.close
  }
}