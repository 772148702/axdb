//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

mixin BufUtil {
  static Str bufToStr(Buf? val, Str defVal:="") {
    str := ""
    if (val != null && val.size > 0) {
      val.seek(0)
      try {
        while (val.more) {
          str += val.readUtf
        }
      } catch {
        val.seek(0)
        //echo(val.toHex)
        str = val.toStr
      }
    } else {
      str = defVal
    }
    return str
  }

  static Buf strToBuf(Str s) {
    b := Buf()
    b.writeUtf(s)
    b.flip
    return b
  }

  static Void writeBuf(OutStream out, Buf? val) {
    if (val == null) {
      out.writeI4(-1)
      return
    }
    val.seek(0)
    out.writeI4(val.size)
    out.writeBuf(val)
    val.seek(0)
  }

  static Buf? readBuf(InStream in) {
    size := in.readS4
    if (size == -1) {
      return null
    }
    val := Buf(size)
    in.readBuf(val, size)
    val.flip
    return val
  }
}


