//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

class WBItem : BufUtil {
  ** pointer to sub node that less or equals to key
  Int pointer

  Buf key

  Buf? val

  new make(|This| f) { f(this) }

  override Str toStr() {
    return "ptr=$pointer, key=${bufToStr(key)}"
  }
}

class WBNode : BufUtil {
  Int maxSize := 1024
  Int id

  Bool dirty := false
  Bool leaf := true

  private WBItem[]? list := null

  private static const Int headerSize := 8 + 1

  Int byteSize { private set }

  new makeBuf(Int id, Buf buf) {
    this.id = id
    this.byteSize = buf.size
    read(buf)
  }

  static new makeEmpty(Int id, Int maxSize, Bool isLeaf) {
    node := makeList(id, maxSize, [WBItem{ pointer = -1; key = Buf() }], isLeaf)
    return node
  }

  new makeList(Int id, Int maxSize, WBItem[] list, Bool isLeaf) {
    this.id = id
    this.maxSize = maxSize
    this.list = list
    this.leaf = isLeaf
    this.byteSize = 0
    dirty = true
  }

  Int size() { list.size }

  Int minSize() { maxSize/2 }

  override Str toStr() { "id=$id, $list" }

  WBItem get(Int i) { list[i] }

  private Void read(Buf buf) {
    in := buf.in
    maxSize = in.readS4
    size := in.readS4
    leaf = in.readBool

    list = WBItem[,]{capacity = size}
    for (i:=0; i<size; ++i) {
      p := headerSize + i * (8+2)
      in = buf.in
      in.skip(p)

      ptr := in.readS8

      //read Key
      keyOffset := in.readS2
      //echo("$buf, $keyOffset")
      in = buf.in
      in.skip(keyOffset)
      key := readBuf(in)
      val := readBuf(in)
      list.add(WBItem{ it.key = key; it.pointer = ptr; it.val = val })
    }
  }

  Buf toBuf() {
    b := Buf()
    write(b)
    dirty = false
    return b
  }

  private Void write(Buf buf) {
    buf.seek(0)
    buf.writeI4(maxSize)
    buf.writeI4(size)
    buf.writeBool(leaf)

    buf.size = keyBase
    buf.seek(keyBase)
    keyOffset := Int[,]{capacity = list.size}
    list.each {
      keyOffset.add(buf.pos)
      //echo("ptr=$it.pointer, key=$buf.pos:$it.key")
      writeBuf(buf.out, it.key)
      writeBuf(buf.out, it.val)
    }
    buf.flip

    buf.seek(headerSize)
    list.each |v,i|{
      buf.writeI8(v.pointer)
      buf.writeI2(keyOffset[i])
    }
    //echo("$buf, $size")
    this.byteSize = buf.size
  }

  Void insert(Int pos, Buf key, Int ptr, Buf? val := null) {
    if (size > maxSize || pos < 0 || pos >size) {
      throw IndexErr("out size pos=$pos, size=$size, max=$maxSize")
    }
    item := WBItem{ it.key = key; it.pointer = ptr; it.val = val }
    if (pos == size) {
      list.add(item)
    } else {
      list.insert(pos, item)
    }
    //echo("$list")
    dirty = true
  }

  Void set(Int pos, Buf key, Int ptr, Buf? val := null) {
    if (pos >= size || pos < 0) {
      throw IndexErr("out size pos=$pos, size=$size")
    }
    item := list[pos]
    item.key = key
    item.pointer = ptr
    item.val = val
    dirty = true
  }

  Void removeAt(Int pos) {
    list.removeAt(pos)
    dirty = true
  }

  Void insertAll(WBItem[] items, Bool isLeft) {
    if (isLeft) {
      list.insertAll(0, items)
    } else {
      list.insertAll(size-1, items)
    }
    dirty = true
  }

  WBItem[] removeAll(Bool isLeft) {
    removeAt := (size - minSize + 1) / 2 + minSize
    WBItem[]? tlist
    if (isLeft) {
      tlist = list[0..<removeAt]
      list = list[removeAt..-1]
    } else {
      tlist = list[removeAt..-2]
      slist := list[0..<removeAt]
      slist.add(list.last)
      list = slist
    }
    dirty = true
    return tlist
  }

  WBNode split(Int newNodeId) {
    left := size / 2

    tlist := list[left..-1]
    node := makeList(newNodeId, maxSize, tlist, leaf)

    list = list[0..<left]
    if (leaf) {
      last := WBItem{ it.key = Buf(); it.pointer = newNodeId; it.val = null }
      list.add(last)
    }
    dirty = true
    //echo("$list | $tlist")
    return node
  }

  private Int keyBase() {
    p := headerSize + (maxSize)*(8+2)
    return p
  }

  Buf greater() {
    if (leaf) {
      return list[size-2].key
    }
    return list.last.key
  }
}

