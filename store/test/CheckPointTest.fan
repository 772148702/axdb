
class CheckPointTest : Test {
  File path := File(`data/`)
  Str name := "data"

  override Void setup() {
    path.delete
  }

  private Str readPageContent(PageMgr store, Int transId, Int pageId) {
     page := store.getPage(transId, pageId)
     page.buf.seek(0)
     in := page.buf.in
     str := in.readUtf
     return str
  }

  private Void writePageContent(PageMgr store, Int transId, Page page, Str str) {
     page.verifyBuf
     page.buf.seek(0)
     out := page.buf.out
     out.writeUtf(str)
     out.close
     page.verifyBuf
     store.updatePage(transId, page)
  }

  private Void read(Int transId, Str p1, Str p2) {
     store := TransPageMgr(path, name)
     store.dump
     store.begin(transId)

     str := readPageContent(store, transId, 0)
     verifyEq(str, p1)

     str2 := readPageContent(store, transId,  1)
     verifyEq(str2, p2)

     store.close
  }

  private Void write2() {
     Int transId := 1
     Int transId2 := 2

     store := TransPageMgr(path, name)
     store.begin(transId)

     page := store.createPage(transId)
     writePageContent(store, transId, page, "Hello")
     store.startCheckPoint

     store.begin(transId2)
     page = store.createPage(transId2)
     writePageContent(store, transId2, page, "Hello2")

     page = store.createPage(transId)
     writePageContent(store, transId, page, "World")

     store.commit(transId)

     page = store.createPage(transId2)
     writePageContent(store, transId2, page, "World2")

     store.commit(transId2)

     store.close
     store.dump
     echo
  }

  Void test() {
    write2

    echo("--")

    store := TransPageMgr(path, name)
    store.dump
    store.recover

    echo("--")

    str := readPageContent(store, 3, 2)
    verifyEq(str, "World")

    str = readPageContent(store, 3, 3)
    verifyEq(str, "World2")

    store.close
    read(3, "Hello", "Hello2")
  }
}