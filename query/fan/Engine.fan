//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using axdbStore

internal class DbBTree : BTree {
  internal StoreClient store

  new make(StoreClient store) : super.make() {
    this.store = store
  }

  override Buf readNode(Int transId, Int id) {
    b := store.read(transId, id)
    return b.buf
  }
  override Int createNode(Int transId) {
    s := store.create(transId)
    return s.id
  }
  override Void updateBuf(Int transId, Int id, Buf buf) {
    ok := store.reqWrite(transId, id)
    if (ok) {
      b := Block(id, buf, transId, -1)
      store.write(transId, b)
      //echo("updateBuf: $id, $buf.toHex")
    }
  }
}

class Engine {
  const StoreClient store
  TableMeta tableMeta := TableMeta()
  private Executor executor

  new make(File path, Str name) {
    store = StoreClient(path, name)
    init
    executor = Executor(this)
  }

  new makeStore(StoreClient store) {
    this.store = store
    init
    executor = Executor(this)
  }

  Void close() {
    store.close
  }

  Obj?[] exeSql(Str sql) {
    executor.exeSql(sql)
  }

  private Void init() {
    transId := transact(null, TransState.begin)
    block := store.read(transId, 0)
    if (block == null) {
      Executor.log.debug("init new database")
      block = store.create(transId)
      buf := Buf()
      tableMeta.write(buf.out)
      buf.flip
      store.reqWrite(transId, block.id)
      block = block.dupWith { it.buf = buf.toImmutable  }
      store.write(transId, block)
    }
    else {
      tableMeta.read(block.buf.in)
    }
    transact(transId, TransState.commit)
  }

  private Void saveTableMeta(Int transId) {
    block := store.read(transId, 0)
    buf := Buf()
    tableMeta.write(buf.out)
    buf.flip
    store.reqWrite(transId, block.id)
    block = block.dupWith { it.buf = buf.toImmutable  }
    store.write(transId, block)
  }

  Bool createTable(Int transId, CreateStmt stmt) {
    tab := tableMeta[stmt.table]
    if (tab != null) return false

    btree := DbBTree(store)
    btree.initRoot(transId)

    tab = Table {
      it.name = stmt.table
      it.type = stmt.type
      it.fields = stmt.fields
      it.key = stmt.key
      it.root = btree.root.id
    }
    tableMeta.map[tab.name] = tab
    saveTableMeta(transId)
    return true
  }

  Void insert(Int transId, Str table, Buf key, Buf val) {
    tab := tableMeta[table]
    if (tab == null) {
      throw ArgErr("table $table not found")
    }
    btree := DbBTree(store).initRoot(transId, tab.root)
    btree.insert(transId, key, -1, val)
  }

  Buf? search(Int transId, Str table, Buf key) {
    tab := tableMeta[table]
    if (tab == null) {
      throw ArgErr("table $table not found")
    }
    btree := DbBTree(store).initRoot(transId, tab.root)
    res := btree.search(transId, key)
    return res.val
  }

  Bool remove(Int transId, Str table, Buf key) {
    tab := tableMeta[table]
    btree := DbBTree(store).initRoot(transId, tab.root)
    return btree.remove(transId, key)
  }

  Int transact(Int? transId, TransState state) {
    return store.transact(transId, state)
  }
}