//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

class BlockStore {
  TransPageMgr pageStore
  private const File lockFile

  new make(File path, Str name) {
    lockFile = path + `${name}.lck`
    if (lockFile.exists) {
      str := lockFile.in.readUtf
      throw Err("database already opened: path=$path,name=$name,$str")
    }
    lockFile.out.writeUtf(DateTime.now.toStr).close
    lockFile.deleteOnExit

    pageStore = TransPageMgr(path, name)
    pageStore.recover
    //pageStore.logger->logFile->dump
  }

  private Block? read(Int transId, Int pageId) {
    buf := Buf()
    page := pageStore.getPage(transId, pageId)
    if (page == null) {
      //echo("page $pageId, trans:$transId")
      return null
    }
    page.buf.seek(0)
    buf.writeBuf(page.buf)
    firstPage := page

    while (page.nextPageId != Page.invalidId) {
      page = pageStore.getPage(transId, page.nextPageId)
      buf.writeBuf(page.buf)
    }

    buf.flip

    size := buf.readS4
    uncompBuf := Buf(size)
    buf.readBuf(uncompBuf, size)
    block := Block(pageId, uncompBuf, firstPage.transId, firstPage.versionPageId)
    return block
  }

  Bool reqWrite(Int transId, Int blockId) {
     return pageStore.reqWrite(transId, blockId)
  }

  private Void writeBuf(Int transId, Int blockId, Buf buf) {
    pageStore.verifyLock(transId, blockId)

    oldPage := pageStore.getPage(transId, blockId)
    newPage := Page.makeDup(oldPage)
    buf.seek(0)

    Page curPage := newPage
    while (true) {
      if (buf.remaining < newPage.contentSize) {
        curPage.buf.seek(0)
        curPage.buf.writeBuf(buf, buf.remaining)
        curPage.nextPageId = Page.invalidId

        if (curPage == newPage && !newPage.dangling) {
          pageStore.updatePage(transId, curPage, oldPage)
        } else {
          pageStore.updatePage(transId, curPage)
        }

        break
      } else {
        curPage.buf.seek(0)
        curPage.buf.writeBuf(buf, newPage.contentSize)
        temPage := pageStore.createPage(transId)
        curPage.nextPageId = temPage.id

        if (curPage == newPage && !newPage.dangling) {
          pageStore.updatePage(transId, curPage, oldPage)
        } else {
          pageStore.updatePage(transId, curPage)
        }
        curPage = temPage
      }
    }
  }

  Void write(Int transId, Block nblock) {
    buf := Buf(nblock.buf.size+4)
    buf.writeI4(nblock.buf.size)
    buf.writeBuf(nblock.buf)
    writeBuf(transId, nblock.id, buf)
    //nblock.transId = transId
    //nblock.dirty = false
  }

  Int create(Int transId) {
    page := pageStore.createPage(transId)
    pageStore.reqWrite(transId, page.id)
    return page.id
  }

  Void delete(Int transId, Int blockId) {
    pageStore.verifyLock(transId, blockId)
    page := pageStore.getPage(transId, blockId)
    last := pageStore.getLinkLastPage(transId, page)
    pageStore.deletePage(transId, page, last)
  }

  Int transact(Int? transId, TransState state) {
    switch (state) {
      case TransState.begin:
        transId = pageStore.begin(transId)
      case TransState.prepare:
        pageStore.prepare(transId)
      case TransState.commit:
        pageStore.commit(transId)
      case TransState.abort:
        pageStore.rollback(transId)
    }
    return transId
  }

//  Void sync() {
//    pageStore.flush
//  }

  Void close() {
    pageStore.close
    lockFile.delete
  }
}


