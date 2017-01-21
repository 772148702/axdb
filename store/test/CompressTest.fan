
class CompressTest : Test {

  Void test() {
    buf := Buf().writeUtf("Hello World").flip
    com := Compress { compressType = 0 }
    cbuf := com.compress(buf)
    ubuf := com.uncompress(cbuf.in)

    str := ubuf.readUtf
    verifyEq(str, "Hello World")
    verifyEq(buf.size, ubuf.size)
  }

  Void testCompress0() {
    buf := Buf().writeUtf("Hello World").flip
    com := Compress { checkCode = null }
    cbuf := com.compress(buf)
    ubuf := com.uncompress(cbuf.in)

    str := ubuf.readUtf
    verifyEq(str, "Hello World")
    verifyEq(buf.size, ubuf.size)
  }

  Void testCompress() {
    buf := Buf().writeUtf("Hello World").flip
    com := Compress {}
    cbuf := com.compress(buf)
    ubuf := com.uncompress(cbuf.in)

    str := ubuf.readUtf
    verifyEq(str, "Hello World")
    verifyEq(buf.size, ubuf.size)
  }

  Void testCompress2() {
    buf := Buf()
    100.times { buf.writeUtf("Hello World") }
    buf.flip

    size := buf.size

    com := Compress { compressType = 1 }
    cbuf := com.compress(buf)
    ubuf := com.uncompress(cbuf.in)

    str := ubuf.readUtf
    verifyEq(str, "Hello World")
    verifyEq(buf.size, ubuf.size)
  }
}