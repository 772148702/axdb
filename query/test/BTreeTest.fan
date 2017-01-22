
internal class MyTree : BTree {
  private Buf[] list := Buf[,]

  new make() : super.make() {}

  override Buf readNode(Int transId, Int id) {
    list[id]
  }
  override Int createNode(Int transId) {
    s := list.size
    list.add(Buf())
    return s
  }
  override Void updateBuf(Int transId, Int id, Buf buf) {
    list[id] = buf
    //echo("updateBuf: $id, $buf.toHex")
  }
}

class BTreeTest : Test, BufUtil {
  static const Int transId := 0

  private static Void insert(MyTree tree, Int i) {
    tree.insert(transId, strToBuf("key$i"), i, strToBuf("value$i"))
  }

  static Void test() {
    tree := MyTree { maxKeySize = 4 }
    tree.initRoot(transId)

    list := Int[,]
    100.times {
      list.add(it)
    }
    list.shuffle
    echo(list)

    //list := [0,2,1]

    list.each {
      insert(tree, it)
    }

    //tree.insert(strToBuf("key2"), 2, strToBuf("v2"))
    //tree.insert(strToBuf("key2"), 2, strToBuf("val2"), true)
    tree.dump(transId)
    Env.cur.out.print("scan:")
    tree.scan(transId) |i,v| {
      Env.cur.out.print("$i,")
    }
    Env.cur.out.print("\n")

    echo("==========")
    r := tree.search(transId, strToBuf("key2"))
    echo("$r")
  }
}