
class TransTest : Test {
  File path := File(`data/`)
  Str name := "data"

  override Void setup() {
    path.delete
  }

  private Str readPageContent(PageMgr store, Int transId, Int pageId) {
     page := store.getPage(transId, pageId)
     in := page.buf.in
     str := in.readUtf
     return str
  }

  private Void writePageContent(PageMgr store, Int transId, Page page, Str str) {
     page.verifyBuf
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

  private Void write() {

     Int transId := 0

     store := TransPageMgr(path, name)
     store.begin(transId)

     page := store.createPage(transId)
     writePageContent(store, transId, page, "Hello")

     page2 := store.createPage(transId)
     writePageContent(store, transId, page2, "World")

     store.commit(transId)

     store.close
     store.dump
     echo("--")
  }

  Void test() {
    write
    read(3, "Hello", "World")
  }

  private Void update() {
     Int transId := 2

     store := TransPageMgr(path, name)
     store.begin(transId)

     page := store.getPage(transId, 0)
     writePageContent(store, transId, page, "Update")

     store.commit(transId)

     store.close
     store.dump
  }

  Void testUpdate() {
    write
    update
    read(3, "Update", "World")
  }

  private Void delete() {
     Int transId := 2

     store := TransPageMgr(path, name)
     store.begin(transId)

     page := store.getPage(transId, 0)
     store.deletePage(transId, page, page)

     page2 := store.createPage(transId)
     writePageContent(store, transId, page2, "Delete")

     store.commit(transId)

     store.close
     store.dump
  }

  Void testDelete() {
    write
    delete
    read(3, "Delete", "World")
  }

  private Void version() {
     Int transId := 2

     store := TransPageMgr(path, name)
     store.begin(transId)

     page := store.getPage(transId, 0)
     store.deletePage(transId, page, page)

     page2 := store.createPage(transId)
     writePageContent(store, transId, page, "Delete")
     writePageContent(store, transId, page, "Version")

     store.flush

     store.begin(0)
     str := readPageContent(store, 0, 0)
     verifyEq("Hello", str)

     str = readPageContent(store, transId, 0)
     verifyEq("Version", str)

     store.commit(transId)

     store.close
     store.dump
  }

  Void testVersion() {
    write
    version
  }

}