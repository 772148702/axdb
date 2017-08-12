
class StoreClientTest : Test {
  File path := File(`data/`)
  Str name := "data"

  override Void setup() {
    path.delete
  }

  private Void write() {
    store := StoreClient(path, name)
    transId := store.begin
    block := store.create(transId)
    buf := Buf().writeUtf("Hello").flip
    //echo("$buf, $buf.toHex")
    store.write(transId, block.dupWith{ it.buf = buf })

    block2 := store.create(transId)
    buf2 := Buf().writeUtf("World").flip
    store.write(transId, block2.dupWith{ it.buf = buf2 })

    //echo("$block.id, $block2.id")

    store.transact(transId, TransState.commit)
    store.close
  }

  private Void update() {
    store := StoreClient(path, name)
    transId := store.begin

    block := store.read(transId, 0)
    str := block.buf.in.readUtf
    //echo("$block.buf, $block.buf.toHex")
    verifyEq(str, "Hello")

    buf := Buf().writeUtf("Update").flip
    store.reqWrite(transId, block.id)
    store.write(transId, block.dupWith{ it.buf = buf })

    block2 := store.read(transId, 1)
    str2 := block2.buf.in.readUtf
    verifyEq(str2, "World")
    store.transact(transId, TransState.commit)

    transId = store.begin
    store.reqWrite(transId, block2.id)
    store.delete(transId, block2.id)
    store.transact(transId, TransState.abort)

    store.close
  }

  private Void read(Int blockId, Str expected) {
    store := StoreClient(path, name)
    transId := store.begin
    block := store.read(transId, blockId)

    str := block.buf.in.readUtf
    verifyEq(str, expected)

    store.transact(transId, TransState.commit)
    store.close
  }

  Void test() {
    write
    echo("---")
    update
    echo("---")
    read(0, "Update")
  }

  Void testRead() {
    write

    store := StoreClient(path, name)
    transId := -1
    block := store.read(transId, 0)
    str := block.buf.in.readUtf
    verifyEq(str, "Hello")
    store.close
  }

  Void testWrite() {
    store := StoreClient(path, name)
    transId := store.begin
    block := store.create(transId)
    buf := Buf()
    sb := StrBuf()
    10000.times { sb.add("$it,") }
    buf.writeUtf(sb.toStr)
    buf.flip
    //echo("$buf, $buf.toHex")
    store.write(transId, block.dupWith{ it.buf = buf })
    store.transact(transId, TransState.commit)
    store.close

    store = StoreClient(path, name)
    transId = -1
    block = store.read(transId, 0)
    str := block.buf.in.readUtf
    verify(str.startsWith("0,"))
    verify(str.endsWith("9999,"))
    store.close
  }
}