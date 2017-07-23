//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

**
** PageMgr is a base page store
**
abstract class PageMgr {
  ** byte num of a page
  Int pageSize := 1024
  Int pageCount := 0
  Int version := 0
  internal Int freePage := Page.invalidId

  static const Int headerSize := 100

  static const Log log := PageMgr#.pod.log

  Str name := ""

  protected abstract InStream? storeIn(Int pageId)

  protected abstract OutStream storeOut(Int pageId)

  protected Void init(Bool newCreate) {
    if (newCreate) {
      out := storeOut(Page.invalidId)
      writeHeader(out)
      out.flush
    } else {
      in := storeIn(Page.invalidId)
      readHeader(in)
    }
  }

  Void dump() {
    echo("pageSize:$pageSize, pageCount:$pageCount")
  }

  private Void readHeader(InStream in) {
    version = in.readS4
    pageSize = in.readS4
    pageCount = in.readS8
    freePage = in.readS8
    code := in.readS8
    if (code != checkCode) {
      throw Err("check code error")
    }
  }

  private Int checkCode() {
    Int c := version
    c = c * 31 + pageSize
    c = c * 31 + pageCount
    c = c * 31 + freePage
    return c
  }

  private Void writeHeader(OutStream out) {
    out.writeI4(version)
    out.writeI4(pageSize)
    out.writeI8(pageCount)
    out.writeI8(freePage)
    out.writeI8(checkCode)
  }

  virtual Void close() { flush }
  virtual Void flush() {
    out := storeOut(-1)
    writeHeader(out)
  }

  virtual Void updatePage(Int transId, Page page, Page? old := null, Bool backup := true) {
    page.transId = transId
    page.dirty = true
    flushPage(page)
  }

  ** flush the page buf to parent buf
  protected virtual Void flushPage(Page page) {
    if (!page.dirty) {
      return
    }
    log.debug("flushPage:$page.id")
    pageId := page.id
    out := storeOut(pageId)
    page.write(out)

    page.dirty = false
    page.dangling = false
  }

  ** create new empty page
  virtual Page createPage(Int transId) {
    if (freePage != Page.invalidId) {
      page := getPage(transId, freePage)
      freePage = page.nextPageId
      page.nextPageId = Page.invalidId
      return page
    }

    log.debug("new page $pageCount")
    page := Page(this.pageSize, pageCount, null)
    ++pageCount

    return page
  }

  ** get page by page ID
  virtual Page? getPage(Int transId, Int pageId) {
    loadPage(pageId)
  }

  protected virtual Page? loadPage(Int pageId) {
    if (pageId != 0 && pageId >= pageCount) {
      log.debug("pageId error: $pageId >= $pageCount")
      return null
    }
    in := storeIn(pageId)
    if (in == null) {
      return null
    }

    return Page(this.pageSize, pageId, in)
  }

  Page getLinkLastPage(Int transId, Page page) {
    pid := page.nextPageId
    Page p := page
    while (pid != Page.invalidId) {
      p = getPage(transId, pid)
      pid = p.nextPageId
    }
    return p
  }

  virtual Void deletePage(Int transId, Page first, Page last := first) {
    last.nextPageId = freePage
    updatePage(transId, last)
    freePage = first.id
  }
}

**
** buffered page store
**
abstract class BufferedPageMgr : PageMgr {
  LruCache cache

  new make() {
    cache = LruCache(1000) {
      canRemoveItem = |Page p->Bool| { !p.dirty }
      onRemoveItem = |Page p| { flushPage(p) }
    }
  }

  override Void close() {
    cache.clear
    super.close
  }

  override Page createPage(Int transId) {
    page := super.createPage(transId)
    cache.set(page.id, page)
    return page
  }

  override Page? getPage(Int transId, Int pageId) {
    return getPageDirect(pageId)
  }

  virtual Page? getPageDirect(Int pageId) {
    item := cache.getItem(pageId)
    if (item != null) {
      return item.val
    }
    page := loadPage(pageId)
    cache.set(pageId, page)
    return page
  }

  override Void updatePage(Int transId, Page page, Page? old := null, Bool backup := true) {
    page.transId = transId
    page.dirty = true
    updatePageDirect(page)
  }

  virtual Void updatePageDirect(Page page) {
    page.dirty = true
    if (cache.get(page.id) == null) {
      //out cache
      beforeFlush
      flushPage(page)
    } else {
      cache.set(page.id, page)
    }
  }

  virtual Void deletePageDirect(Page first, Page last) {
    last.nextPageId = freePage
    updatePageDirect(last)
    freePage = first.id
  }

  protected virtual Void beforeFlush() {}

  override Void flush() {
    beforeFlush
    log.debug("flush all buffer")
    cache.each |Page p| {
      //echo("will flushPage")
      flushPage(p)
    }
    super.flush
  }
}

**
** in memory page store
**
class BufPageMgr : PageMgr {
  Buf buf

  new make(Buf buf) {
    this.buf = buf
    init(buf.size == 0)
  }

  protected override InStream? storeIn(Int pageId) {
    if (pageId == Page.invalidId) {
      buf.seek(0)
      return buf.in
    }
    pos := pageId * pageSize + headerSize
    if (pos >= buf.size) {
      return null
    }
    buf.seek(pos)
    return buf.in
  }

  protected override OutStream storeOut(Int pageId) {
    if (pageId == Page.invalidId) {
      buf.seek(0)
      return buf.out
    }
    pos := pageId * pageSize + headerSize
    buf.seek(pos)
    return buf.out
  }

  override Void flush() {
    super.flush
    buf.sync
  }

  override Void close() {
    super.close
    buf.close
  }
}

