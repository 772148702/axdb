//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

**
** Store API
**
const class StoreClient {

  private const CacheActor cache
  private const StoreActor store
  private const Compress compress := Compress { checkCode = null }

  new make(File path, Str name) {
    cache = CacheActor(1000)
    store = StoreActor(path, name)
  }

  Void close() {
    store->send_close->get
  }

  private Block getVersionBlcok(Int transId, Block block) {
     if (block.transId == transId) {
       return block
     }
     if (block.versionId == Page.invalidId) {
       return block
     }
     if (cache.hasTrans(block.transId)) {
       return block
     }

     if (block.version == null) {
       block = block.dupWith { version = doRead(block.versionId) }
     }
     return block.version
  }

  private Block doRead(Int blockId) {
    Block b := store->send_read(Page.invalidId, blockId)->get
    return b.dupWith { it.buf = compress.uncompress(it.buf.in) }
  }

  Block? read(Int transId, Int blockId) {
    Block? block := cache.getCache(blockId)
    if (block == null) {
      block = doRead(blockId)
      cache.setCache(block.id, block)
    }

    return getVersionBlcok(transId, block)
  }

  Bool reqWrite(Int transId, Int blockId) {
    store->send_reqWrite(transId, blockId)->get
  }

  Void write(Int transId, Block block) {
    block = block.dupWith { it.version = null }
    cache.setCache(transId, block)
    b := block.dupWith { it.buf = compress.compress(it.buf) }
    store->send_write(transId, b)
  }

  Block create(Int transId) {
    id := store->send_create(transId)->get
    block := Block(id, Buf(), transId, Page.invalidId)
    cache.setCache(transId, block)
    return block
  }

  Void delete(Int transId, Int blockId) {
    store->send_delete(transId, blockId)
  }

  Int begin(Int? transId := null) {
    transId = cache.beginTrans(transId)
    store->send_begin(transId)
    return transId
  }

  Void rollback(Int transId) {
    cache.endTrans(transId)
    store->send_rollback(transId)
  }

  Void commit(Int transId) {
    cache.endTrans(transId)
    store->send_commit(transId)->get
  }

  Bool prepare(Int transId) {
    store->send_prepare(transId)->get
  }
}

