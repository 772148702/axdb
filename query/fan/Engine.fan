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

  new make(File path, Str name) {
    store = StoreClient(path, name)
    init
  }

  new makeStore(StoreClient store) {
    this.store = store
    init
  }

  Void close() {
    store.close
  }

  Obj?[] exeSql(Str sql) {
    Int transId := -1
    pos := sql.index(":")
    if (pos != null) {
      p1 := sql[0..<pos]
      sql = sql[pos+1..-1]
      if (p1.size > 0) {
        transId = p1.toInt
      }
    }
    res := Executor(this).exeSql(sql, transId)
    //echo("engine$res")
    return res
  }

  private Void init() {
    transId := transact(-1, TransState.begin)
    //echo("transId$transId")
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

  Bool removeTable(Int transId, DropStmt stmt) {
    tab := tableMeta[stmt.table]
    if (tab == null) {
      return false
    }
    btree := DbBTree(store).initRoot(transId, tab.root)
    //echo("------1")
    btree.visitNode(transId) |id| {
      ok := store.reqWrite(transId, id)
      if (!ok) throw Err("lock fail")
      store.delete(transId, id)
    }
    //echo("------2")

    res := tableMeta.map.remove(stmt.table)
    if (res == null) return false
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
    if (btree.root.id != tab.root) {
      tab.root = btree.root.id
      saveTableMeta(transId)
    }
  }

  BTreeIterator scan(Int transId, Str table) {
    tab := tableMeta[table]
    if (tab == null) {
      throw ArgErr("table $table not found")
    }
    btree := DbBTree(store).initRoot(transId, tab.root)
    itr := BTreeIterator(btree, transId)
    return itr
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
    res := btree.remove(transId, key)
    if (btree.root.id != tab.root) {
      tab.root = btree.root.id
      saveTableMeta(transId)
    }
    return res
  }

  Int transact(Int transId, TransState state) {
    return store.transact(transId, state)
  }
}