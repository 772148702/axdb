//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

const class Compress {
  const Int compressType := 1
  const Str? checkCode := "CRC-32"
  const Int compressLimit := 32

  new make(|This| f) { f(this) }

  virtual InStream inCompress(InStream in, Int comType := compressType) {
    InStream result := in
    switch (comType) {
      case 1:
        result = Zip.gzipInStream(in)
      case 2:
        result = Zip.deflateInStream(in)
      default:
    }
    return result
  }

  virtual OutStream outCompress(OutStream out, Int comType := compressType) {
    OutStream result := out
    switch (comType) {
      case 1:
        result = Zip.gzipOutStream(out)
      case 2:
        result = Zip.deflateOutStream(out)
      default:
    }
    return result
  }

  Buf compress(Buf inBuf) {
    //inBuf.seek(0)
    buf := Buf()
    out := buf.out
    if (inBuf.size == 0) return buf
    //out.writeI4(0)
    out.writeI4(inBuf.size)
    comType := inBuf.size > compressLimit ? compressType : 0
    out.writeI4(comType)

    if (checkCode != null) {
       Int code := inBuf.crc(checkCode)
       out.writeI8(code)
       inBuf.seek(0)
    }

    cout := outCompress(out, comType)
    cout.writeBuf(inBuf)
    cout.flush
    cout.close

    buf.flip

    return buf
  }

  Buf? uncompress(InStream in) {
    //uncompress
    Int code := 0
    //comSize := in.readS4
    srcSize := in.readS4
    if (srcSize < 0) {
      return null
    }
    comType := in.readS4

    if (checkCode != null) {
      code = in.readS8
    }

    cin := inCompress(in, comType)
    outBuf := cin.readBufFully(null, srcSize)
    cin.close

    if (outBuf.size != srcSize) {
      throw Err("Error: readSize:$outBuf.size != srcSize:$srcSize, $outBuf")
    }

    if (checkCode != null) {
      code2 := outBuf.crc(checkCode)
      if (code != code2) {
        throw Err("CRC Error: $code != $code2, bufsize:$outBuf.size")
      }
      outBuf.seek(0)
    }
    //echo("uncompress $outBuf")
    return outBuf
  }
}