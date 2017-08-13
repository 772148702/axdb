
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
    com := Compress { compressType = 1; checkCode = null }
    cbuf := com.compress(buf)
    ubuf := com.uncompress(cbuf.in)

    str := ubuf.readUtf
    verifyEq(str, "Hello World")
    verifyEq(buf.size, ubuf.size)
  }

  Void testCompress() {
    buf := Buf().writeUtf("Hello World").flip
    com := Compress { compressType = 1; compressLimit = 3 }
    cbuf := com.compress(buf)
    ubuf := com.uncompress(cbuf.in)

    str := ubuf.readUtf
    verifyEq(str, "Hello World")
    verifyEq(buf.size, ubuf.size)
  }

  Void testCompress2() {
    buf := Buf().writeUtf("Hello World").flip
    size := buf.size

    com := Compress { compressType = 1 }
    cbuf := com.compress(buf)
    ubuf := com.uncompress(cbuf.in)

    str := ubuf.readUtf
    verifyEq(str, "Hello World")
    verifyEq(buf.size, ubuf.size)
  }

  Void testCompressBig() {
    buf := Buf()
    sb := StrBuf()
    10000.times { sb.add("$it,") }
    buf.writeUtf(sb.toStr)
    buf.flip

    size := buf.size

    com := Compress { compressType = 1; compressLimit = 3 }
    echo("$buf")
    cbuf := com.compress(buf)
    echo("$cbuf")
    ubuf := com.uncompress(cbuf.in)

    str := ubuf.readUtf
    verify(str.startsWith("0,"))
    verify(str.endsWith("9999,"))
    verifyEq(buf.size, ubuf.size)
  }

  Void testZip() {

    buf := Buf()
    sb := StrBuf()
    1000.times { sb.add("$it,") }
    buf.writeUtf(sb.toStr)
    buf.flip

    size := buf.size
    echo(buf)

    outBuf := Buf()
    out := Zip.gzipOutStream(outBuf.out)
    out.writeBuf(buf)

    //out.writeUtf(sb.toStr)

    out.close
    outBuf.flip
    //echo("$buf => $outBuf")

    resBuf := Buf()
    in := Zip.gzipInStream(outBuf.in)
    in.readBufFully(resBuf, size)
    verifyEq(size, resBuf.size)
    //echo(in.readUtf)
  }
}