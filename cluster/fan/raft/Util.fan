//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

class Util {
  static Str[] split(Str str, Str sp, Int max := Int.maxVal) {
    if (sp.size == 0) {
      return [str]
    }
    res := Str[,]
    while (true) {
      if (res.size == max-1) {
        res.add(str)
        break
      }
      i := str.index(sp)
      if (i == null) {
        res.add(str)
        break
      }

      part := str[0..<i]
      res.add(part)

      start := i + sp.size
      if (start < str.size) {
        str = str[start..-1]
      } else {
        str = ""
      }
    }

    return res
  }
}

