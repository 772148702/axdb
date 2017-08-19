//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//


enum class TransState {
  begin,
  prepare,
  commit,
  abort
}

**
** transaction session
**
class Session {
  Int firstFree := Page.invalidId
  Int lastFree := Page.invalidId
  Int lastTime := 0

  TransState state := TransState.begin
}


**
** page store with transaction
**
class TransPageMgr : LogPageMgr {
  private [Int:Session] sessionMap := [:]
  private [Int:Int] lockMap := [:]

  private Int[]? checkPointTrans
  Int lastTrans := -1
  private Int transCount := 0

  new make(File path, Str name) : super(path, name) {
  }

  Bool transCompleted(Int transId) {
    return !sessionMap.containsKey(transId)
  }

  override Page? getPage(Int transId, Int pageId) {
    if (pageId == -1) {
      return null
    }

    if (transId >= 0) {
      ses := sessionMap[transId]
      if (ses != null) {
        ses.lastTime = Duration.nowTicks
      }
    }

    page := super.getPage(transId, pageId)
    if (page == null) {
      return null
    }

    if (page.versionPageId == Page.invalidId
      || transId == Page.invalidId) {
      return page
    }

    if (page.transId == transId) {
      return page
    }

    if (transCompleted(page.transId)) {
      return page
    }

    return getPage(transId, page.versionPageId)
  }

  override Page createPage(Int transId) {
    ses := sessionMap[transId]
    if (ses.firstFree != Page.invalidId) {
      page := getPage(transId, ses.firstFree)
      ses.firstFree = page.nextPageId
      page.nextPageId = Page.invalidId
      updatePage(transId, page)
      return page
    }
    ses.lastTime = Duration.nowTicks

    p := super.createPage(transId)
    //echo("create Page $p.id")
    return p
  }

  Bool reqWrite(Int transId, Int pageId) {
     if (transId == -1) {
       throw ArgErr("not begin trans")
     }
     if (lockMap.containsKey(pageId)) {
        return lockMap[pageId] == transId
     }
     lockMap[pageId] = transId
     return true
  }

  Void verifyLock(Int transId, Int pageId) {
    if (lockMap[pageId] != transId)
      throw ArgErr("lock error: transId=$transId, pageId=$pageId")
  }

  override Void updatePage(Int transId, Page page, Page? old := null, Bool backup := true) {
    if (!page.dangling && old == null) {
      old = loadPage(page.id)
      if (old == null) {
        throw Err("not found old page:$page.id")
      }
    }

    if (old != null && backup && old.transId != transId) {
      Page? verPage
      if (page.versionPageId != Page.invalidId) {
        verPage = getPage(transId, page.versionPageId)
      } else {
        verPage = createPage(transId)
        page.versionPageId = verPage.id
      }
      verOld := Page.makeDup(verPage)
      old.copyTo(verPage)
      updatePage(transId, verPage, verOld, false)
    }

    super.updatePage(transId, page, old, backup)
  }

  override Void deletePage(Int transId, Page first, Page last := first) {
    ses := sessionMap[transId]
    //echo("delete Page $first.id")

    if (ses.firstFree == Page.invalidId) {
      ses.firstFree = first.id
      ses.lastFree = last.id
      return
    }

    last.nextPageId = ses.firstFree
    ses.firstFree = first.id
    updatePage(transId, last)
  }

  Int begin(Int transId) {
    if (transId != -1) {
       lastTrans = transId
    } else {
      transId = ++lastTrans
    }

    if (transId < 0) {
      throw ArgErr("transId error: $transId")
    }

    ++transCount
    if (transCount > 1000) {
      startCheckPoint
      transCount = 0
    }

    logger.begin(transId)
    sessionMap[transId] = Session { lastTime = Duration.nowTicks }
    return transId
  }

  Void rollback(Int transId) {
    recover := Recover(this)
    recover.undo(transId)

    logger.abort(transId)
    ses := sessionMap[transId]
    ses.state = TransState.prepare
    finishTrans(transId)
  }

  Void startCheckPoint() {
    flush
    checkPointTrans = sessionMap.keys
    logger.startCheckPoint(checkPointTrans)
  }

  private Void checkCheckPoint() {
    if (checkPointTrans == null) return

    timeout := Int[,]
    for (i:=0; i<checkPointTrans.size; ++i) {
      trans := checkPointTrans[i]
      if (sessionMap.containsKey(trans)) {
        //echo("unfinished $checkPointTrans[i]")
        if (Duration.nowTicks - sessionMap[trans].lastTime > 10sec.ticks) {
          timeout.add(trans)
        }
        return
      }
      checkPointTrans.removeAt(i)
      --i
    }

    flush
    logger.endCheckPoint(sessionMap.keys)
    checkPointTrans = null
    timeout.each {
      rollback(it)
    }
  }

  Void recover() {
    r := Recover(this)
    r.redo(logger.checkPoint)
  }

  private Void finishTrans(Int transId) {
    if (transId == -1) {
      throw ArgErr("invalid transId")
    }
    releaseLock(transId)
    sessionMap.remove(transId)
    logger.flush
  }

  Void commit(Int transId) {
    ses := sessionMap[transId]
    if (ses.firstFree != Page.invalidId) {
      firstPage := getPage(transId, ses.firstFree)
      lastPage := getPage(transId, ses.lastFree)
      super.deletePage(transId, firstPage, lastPage)
    }
    logger.commit(transId)
    ses.state = TransState.commit
    finishTrans(transId)

    checkCheckPoint
  }

  private Void releaseLock(Int transId) {
    pageList := Int[,]
    lockMap.each |v,k| {
      if (v == transId) {
        pageList.add(k)
      }
    }
    pageList.each {
      lockMap.remove(it)
    }
  }

  Bool prepare(Int transId) {
    logger.prepare(transId)
    ses := sessionMap[transId]
    ses.state = TransState.prepare
    return true
  }
}