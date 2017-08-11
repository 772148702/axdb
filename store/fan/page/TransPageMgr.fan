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
  [Int:Session] sessionMap := [:]
  [Int:Int] lockMap := [:]

  private Int[]? checkPointTrans
  Int lastTrans := -1

  new make(File path, Str name) : super(path, name) {
  }

  Bool transCompleted(Int transId) {
    return !sessionMap.containsKey(transId)
  }

  override Page? getPage(Int transId, Int pageId) {
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

    return super.createPage(transId)
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

    logger.begin(transId)
    sessionMap[transId] = Session()
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

  Void checkCheckPoint() {
    if (checkPointTrans == null) return

    for (i:=0; i<checkPointTrans.size; ++i) {
      if (sessionMap.containsKey(checkPointTrans[i])) return
      checkPointTrans.removeAt(i)
      --i
    }

    flush
    logger.endCheckPoint(sessionMap.keys)
    checkPointTrans = null
  }

  Void recover() {
    r := Recover(this)
    r.redo(logger.checkPoint)
  }

  private Void finishTrans(Int transId) {
    releaseLock(transId)
    sessionMap.remove(transId)
    checkCheckPoint
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