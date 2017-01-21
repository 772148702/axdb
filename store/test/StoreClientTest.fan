
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

    store.commit(transId)
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
    store.commit(transId)

    transId = store.begin
    store.reqWrite(transId, block2.id)
    store.delete(transId, block2.id)
    store.rollback(transId)

    store.close
  }

  private Void read(Int blockId, Str expected) {
    store := StoreClient(path, name)
    transId := store.begin
    block := store.read(transId, blockId)

    str := block.buf.in.readUtf
    verifyEq(str, expected)

    store.commit(transId)
    store.close
  }

  Void test() {
    write
    echo("---")
    update
    echo("---")
    read(0, "Update")
  }
}