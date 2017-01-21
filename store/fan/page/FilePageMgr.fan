//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

**
** file page store
**
class FilePageMgr : BufferedPageMgr {
  File dir
  [Int:Buf] bufMap := [:]

  ** page num of a block
  Int pagePerBlock := 100 * 1024

  new make(File path, Str name) {

    dir = path
    this.name = name
    if (!dir.exists) {
      dir.create
    }
    File f := getFile(0)
    exists := f.exists
    //echo("$f exists=$exists")
    init(!exists)
  }

  private File getFile(Int fileId) {
    dir + `$name-${fileId}.dat`
  }

  private Buf? getBuf(Int pageId, Bool readMode) {
    fileId := pageId / pagePerBlock
    if (pageId == Page.invalidId) {
      fileId = 0
    }
    buf := bufMap[fileId]
    if (buf == null) {
      file := getFile(fileId)
      buf = file.open
      bufMap[fileId] = buf
    }

    if (pageId == Page.invalidId) {
      buf.seek(0)
      return buf
    }

    //seek buf
    pageId = pageId % pagePerBlock
    pos := pageId * pageSize  + headerSize

    if (readMode && pos >= buf.size) {
      //echo("pageId out: $pos >= $buf.size")
      return null
    }
    buf.seek(pos)
    return buf
  }

  protected override InStream? storeIn(Int pageId) {
    getBuf(pageId, true)?.in
  }

  protected override OutStream storeOut(Int pageId) {
    getBuf(pageId, false).out
  }

  override Void flush() {
    super.flush
    bufMap.each |v, k| {
      v.sync
    }
  }

  override Void close() {
    super.close
    bufMap.each |v, k| {
      v.close
    }
  }
}

**
** page mgr with log
**
class LogPageMgr : FilePageMgr {
  PageLog logger
  new make(File path, Str name) : super(path, name) {
    logger = PageLog(path, name)
  }

  override Page createPage(Int transId) {
    page := super.createPage(transId)
    logger.createPage(transId, page.id, this)

    return page
  }

  override Void updatePage(Int transId, Page page, Page? old := null, Bool backup := true) {
    if (!page.dangling && old == null) {
      old = loadPage(page.id)
      if (old == null) {
        throw Err("not found old page:$page.id")
      }
    }
    logger.updatePage(transId, page, old)
    super.updatePage(transId, page, old, backup)
  }

  protected override Void beforeFlush() {
    logger.flush
    super.beforeFlush
  }

  override Void close() {
    super.close
    logger.close
  }

  override Void deletePage(Int transId, Page first, Page last := first) {
    logger.deletePage(transId, first.id, last.id, this)
    super.deletePage(transId, first, last)
  }
}