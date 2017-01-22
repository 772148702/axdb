//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

class Table {
  Str name := ""
  FieldDefExpr[] fields := [,]
  Str key := "id"
  Str type := "table"

  Int count := 0
  Int root := -1
  
  new make(|This| f) {
    f(this)
  }

  new read(InStream in) {
    name = in.readUtf
    key = in.readUtf
    type = in.readUtf
    count = in.readS8
    root = in.readS8

    fsize := in.readS2
    fields.capacity = fsize
    fsize.times {
      field := in.readUtf
      type := in.readUtf
      f := FieldDefExpr(field, type)
      f.constra = in.readUtf
      fields.add(f)
    }
  }

  Void write(OutStream out) {
    out.writeUtf(name)
    out.writeUtf(key)
    out.writeUtf(type)
    out.writeI8(count)
    out.writeI8(root)

    out.writeI2(fields.size)
    fields.each {
      out.writeUtf(it.field)
      out.writeUtf(it.type)
      out.writeUtf(it.constra)
    }
  }
}

class TableMeta {
  Str:Table map := [:]
  Int version := 0
  Int flag := 0
  
  @Operator
  Table? get(Str name) {
    map[name]
  }

  Void read(InStream in) {
    version = in.readS4
    flag = in.readS4
    tsize := in.readS4
    tsize.times {
      t := Table.read(in)
      map[t.name] = t
    }
  }

  Void write(OutStream out) {
    out.writeI4(version)
    out.writeI4(flag)
    out.writeI4(map.size)
    map.each {
      it.write(out)
    }
  }
}