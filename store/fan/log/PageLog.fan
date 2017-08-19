//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

class PLog {
  private LogFile logFile
  private [Int:Int] lastTransPos := [:]

  private static const Log log := PLog#.pod.log

  private Int checkPointPos := -1

  new make(File dir, Str name) {
    logFile = LogFile(dir, name)
    logFile.open
  }

  Int getTransTail(Int transId) {
    lastTransPos.get(transId, -1)
  }

  Int lastReadPos() { logFile.lastReadPos }

  Void startCheckPoint(Int[] trans) {
    pos := curPos
    checkPointPos = pos

    r := CheckLogRec {
      it.transId = -1
      it.type = PLogType.startCheckPoint
      it.previous = -1
      it.transList = trans
      posList := trans.map { lastTransPos.get(it, -1) }
      it.transPos = posList
    }

    buf := Buf()
    r.write(buf.out)
    buf.flip

    logFile.writeBufObj(buf)

    log.debug("pos:${pos}..$curPos, log:$r")
  }

  Int checkPoint() { logFile.checkPoint }

  Void endCheckPoint(Int[] trans) {
    pos := curPos
    r := CheckLogRec {
      it.transId = -1
      it.type = PLogType.endCheckPoint
      it.previous = pos - checkPointPos
      it.transList = trans
      posList := trans.map { lastTransPos.get(it, -1) }
      it.transPos = posList
    }

    buf := Buf()
    r.write(buf.out)
    buf.flip

    logFile.writeBufObj(buf)

    log.debug("pos:${pos}..$curPos, log:$r")

    logFile.checkPoint = pos
    flush
    logFile.trim(checkPointPos)
    checkPointPos = -1
  }

  private Void finishTrans(Int transId) {
    lastTransPos.remove(transId)
  }

  PLogRec? readLog(Int pos) {
    buf := logFile.readBufObj(pos)
    if (buf == null) return null
    return PLogRec.readRec(buf.in)
  }

  internal Int abortTransOffline(Int transId, Int previous) {
    pos := curPos
    r := PLogRec {
      it.transId = transId
      it.type = PLogType.abortTrans
      it.previous = pos - previous
    }

    buf := Buf()
    r.write(buf.out)
    buf.flip

    logFile.writeBufObj(buf)
    finishTrans(transId)

    log.debug("pos:${pos}(0x${pos.toHex})..$curPos, log:$r")
    return pos
  }

  This add(PLogRec rec) {
    pos := curPos
    prevPos := lastTransPos.get(rec.transId, -1)
    rec.previous = (prevPos != -1) ? pos - prevPos : -1

    buf := Buf()
    rec.write(buf.out)
    buf.flip

    logFile.writeBufObj(buf)

    if (rec.type == PLogType.commitTrans) {
      finishTrans(rec.transId)
    } else {
      lastTransPos[rec.transId] = pos
    }

    log.debug("pos:${pos}(0x${pos.toHex})..$curPos, log:$rec")

    return this
  }

  private Int curPos() {
    logFile.length
  }

  Void flush() {
    logFile.flush
  }

  Void close() {
    flush
    logFile.close
  }

  Void commit(Int transId) {
    r := PLogRec {
      it.transId = transId
      it.type = PLogType.commitTrans
    }
    add(r)
  }

  Void begin(Int transId) {
    if (lastTransPos.containsKey(transId)) {
      throw Err("trans already begin:$transId")
    }
    r := PLogRec {
      it.transId = transId
      it.type = PLogType.beginTrans
    }
    add(r)
  }

  Void prepare(Int transId) {
    r := PLogRec {
      it.transId = transId
      it.type = PLogType.prepareTrans
    }
    add(r)
  }

  Void abort(Int transId) {
    r := PLogRec {
      it.transId = transId
      it.type = PLogType.abortTrans
    }
    add(r)
  }
}

class PageLog : PLog {

  new make(File dir, Str name) : super(dir, name) {
  }

  Void updatePage(Int transId, Page newP, Page? oldP) {
    r := PageLogRec {
      it.transId = transId
      it.type = PLogType.updatePage
      it.newData = newP.fullPageBuf
      it.oldData = oldP?.fullPageBuf
    }

    r {
      it.pageId = newP.id
    }

    add(r)
  }

  Void createPage(Int transId, Int pageId, PageMgr store) {
    r := CreatePageLogRec {
      it.transId = transId
      it.type = PLogType.createPage
      it.pageId = pageId
      it.newFreePage = store.freePage
      it.newPageCount = store.pageCount
    }
    add(r)
  }

  Void deletePage(Int transId, Int first, Int last, PageMgr store) {
    r := FreePageLogRec {
      it.transId = transId
      it.type = PLogType.freePage
      it.firstPage = first
      it.lastPage = last
      it.newFreePage = store.freePage
      it.newPageCount = store.pageCount
    }
    add(r)
  }
}