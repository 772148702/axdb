
class FilePageTest : Test {
  File path := File(`data/`)
  Str name := "data"

  override Void setup() {
    path.delete
  }

  private Void read() {
     store := FilePageMgr(path, name)
     store.dump

     page := store.getPage(0, 0)
     in := page.buf.in
     str := in.readUtf
     verifyEq(str, "Hello")

     page2 := store.getPage(0, 1)
     in2 := page2.buf.in
     str2 := in2.readUtf
     verifyEq(str2, "World")

     store.close
  }

  private Void write() {
     store := FilePageMgr(path, name)
     page := store.createPage(0)
     out := page.buf.out
     out.writeUtf("Hello")
     out.close
     store.updatePage(0, page)

     page2 := store.createPage(0)
     out2 := page2.buf.out
     out2.writeUtf("World")
     out2.close
     store.updatePage(0, page2)

     store.close
     store.dump
     echo("--")
  }

  Void test() {
    write
    read
  }

  Void testEmpty() { 
    store := FilePageMgr(path, name)
    page := store.createPage(0)
    page2 := store.getPage(0, 0)

    verifyEq(page.buf.size, page2.buf.size)
    verifyEq(page.buf.pos, page2.buf.pos)
    verifyEq(page.id, page2.id)
  }
}