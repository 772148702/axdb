//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

class BSResult : BufUtil {
  BSResult? parent
  RBNode node
  Int index
  Int pointer
  Buf? val

  new make(RBNode node, Int ptr, Int ind) {
    this.node = node
    this.pointer = ptr
    this.index = ind
  }

  override Str toStr() {
    return "ptr=$pointer, ind=$index, val=${bufToStr(val)}"
  }
}

const class RBNode : BufUtil {
  const Int maxSize
  const Int size
  const Int id
  const Buf buf
  const Bool leaf

  private static const Int headerSize := 8 + 1

  new make(Int id, Buf buf) {
    this.id = id
    this.buf = buf

    in := buf.in
    maxSize = in.readS4
    size = in.readS4
    leaf = in.readBool
  }

  WBNode toWBNode() {
    WBNode.makeBuf(id, buf)
  }

  BSResult search(Buf key) {
    searchIn(key, 0, size-1)
  }

  virtual Int compareKey(Buf a, Int i) {
    //last is max
    if (i == size-1) {
      return -1
    }
    b := getKey(i)

    a.seek(0)
    b.seek(0)
    while (true) {
      va := a.read
      vb := b.read
      cmp := va <=> vb
      if (cmp != 0) return cmp
      if (va == null || vb == null) return 0
    }
    return 0
  }

  BSResult newSResult(Buf key, Int index) {
    if (!leaf) {
      ptr := getPointer(index)
      result := BSResult(this, ptr, index)
      return result
    } else {
      if (compareKey(key, index) != 0) {
        return BSResult(this, -1, index)
      }
      ptr := getPointer(index)
      result := BSResult(this, ptr, index)
      result.val = getVal(index)
      return result
    }
  }

  ** search in [left,right]
  private BSResult searchIn(Buf key, Int left, Int right) {
    if (left >= right) {
      return newSResult(key, 0)
    }

    index := -1
    while (left < right) {
      middle := left + ((right-left)/2)
      cmp := compareKey(key, middle)
      if (cmp < 0) {
        right = middle
      }
      else if (cmp == 0) {
        index = middle
        break;
      }
      else {
        left = middle+1
      }
      //echo("[$left,$right]")
    }
    if (index == -1) index = left
    return newSResult(key, index)
  }

  private Int getKeyOffset(Int i) {
    if (i >= size || i < 0) {
      throw IndexErr("i=$i")
    }
    p := headerSize + i * (8+2) + 8
    in := buf.in
    in.skip(p)
    return in.readS2
  }

  Int getPointer(Int i) {
    if (i >= size || i < 0) {
      throw IndexErr("i=$i")
    }
    p := headerSize + i * (8+2)
    in := buf.in
    in.skip(p)
    return in.readS8
  }

  private Buf readKey(Int offset) {
    p := offset
    in := buf.in
    in.skip(p)

    return readBuf(in)
  }

  private Buf? readVal(Int offset) {
    p := offset
    in := buf.in
    in.skip(p)

    size := in.readS4
    in.skip(size)
    size = in.readS4
    if (size == -1) return null
    b := Buf(size)
    in.readBuf(b, size)
    return b
  }

  private Int keyBase() {
    p := headerSize + (maxSize)*(8+2)
    return p
  }

  private Buf getKey(Int i) {
    offset := getKeyOffset(i)
    b := readKey(offset)
    b.seek(0)
    return b
  }

  Buf? getVal(Int i) {
    offset := getKeyOffset(i)
    b := readVal(offset)
    b?.seek(0)
    return b
  }

  Void dump(BTree tree, Int level := 0) {
    level.times {
      Env.cur.out.print("  ")
    }
    Env.cur.out.print("id=$id[")
    list := Int[,]
    size.times |i| {
      ptr := getPointer(i)
      keyb := getKey(i)
      keys := bufToStr(keyb)

      valb := getVal(i)
      vals := bufToStr(valb)

      Env.cur.out.print("$ptr$vals $keys; ")
      if (ptr != -1) {
        list.add(ptr)
      }
    }
    Env.cur.out.print("]\n")

    if (!leaf) {
      list.each {
        node := tree.getNode(it)
        node.dump(tree, level+1)
      }
    }
  }
}

