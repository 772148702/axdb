
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
  static const Int transId := -1

  private static Void insert(MyTree tree, Int i) {
    sb := StrBuf()
    sb.add("value$i")
    tree.insert(transId, strToBuf("key$i"), i, strToBuf(sb.toStr))
  }

  Void testScan() {
    tree := MyTree { maxKeySize = 4 }
    tree.initRoot(transId)

    list := Int[,]
    100.times {
      list.add(it)
    }
    list.shuffle
    //echo(list)

    list.each {
      insert(tree, it)
    }

    itr := BTreeIterator(tree, transId)
    Env.cur.out.print("scan:")
    while (itr.more) {
      buf := itr.next
      str := bufToStr(buf)
      Env.cur.out.print("$str,")
    }
    Env.cur.out.print("\n")

    //tree.dump(transId)
    /*
    Env.cur.out.print("scan:")
    tree.scan(transId) |i,v| {
      str := bufToStr(v)
      Env.cur.out.print("$i,$str")
    }
    Env.cur.out.print("\n")
    */
  }

  Void testSearch() {
    tree := MyTree{}
    tree.initRoot(transId)

    list := Int[,]
    1000.times {
      list.add(it)
    }
    list.shuffle
    //echo(list)
    list.each {
      insert(tree, it)
    }

    //tree.dump(transId)

    verifySearch(tree, transId, "key0", "value0")
    verifySearch(tree, transId, "key2", "value2")
    verifySearch(tree, transId, "key500", "value500")
    verifySearch(tree, transId, "key999", "value999")
  }

  private Void verifySearch(MyTree tree, Int transId, Str key, Str val) {
    r := tree.search(transId, strToBuf(key))
    verify(bufToStr(r.val).endsWith(val))
  }
}