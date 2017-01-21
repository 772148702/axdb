//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

abstract class BTree {

  Int maxKeySize := 1024
  Int bufSize := 4096 + ((maxKeySize+1) * 10)

  new make() {
  }

  RBNode? root {
    get {
      if (&root == null) {
        temp := WBNode.makeEmpty(createNode, maxKeySize, true)
        &root = RBNode(temp.id, temp.toBuf.toImmutable)
      }
      return &root
    }
  }

  abstract Buf readNode(Int id)
  abstract Int createNode()
  abstract Void updateBuf(Int id, Buf buf)

  virtual RBNode getNode(Int id) {
    buf := readNode(id)
    node := RBNode(id, buf)
    return node
  }

  private Void updateNode(WBNode node, Bool asRoot := false) {
    ibuf := node.toBuf.toImmutable
    updateBuf(node.id, ibuf)
    if (asRoot || node.id == root.id) {
      root = RBNode(node.id, ibuf)
    }
  }

  BSResult search(Buf key) {
    node := root
    BSResult? result

    while (true) {
      tresult := node.search(key)
      tresult.parent = result
      result = tresult

      if (node.leaf) return result
      if (result.pointer == -1) return result
      node = getNode(result.pointer)
    }

    return result
  }

  Bool remove(Buf key) {
    result := search(key)
    node := result.node.toWBNode
    if (result.pointer == -1 && result.val == null) {
      return false
    }

    node.removeAt(result.index)
    /*
    if (node.size < node.minSize && result.parent != null) {
      parentNode := result.parent.node
      pos := result.parent.index
      ok := false
      if (pos > 0) {
        preNodeId := parentNode.getPointer(pos-1)
        preNode := getNode(preNodeId)
        if (node.size > node.minSize) {
          borrow(node, preNode.toBTNode, true)
          ok = true
        }
      }
      if (!ok && pos < parentNode.size-1) {
        nextNodeId := parentNode.getPointer(pos+1)
        nextNode := getNode(nextNodeId)
        if (node.size > node.minSize) {
          borrow(node, nextNode.toBTNode, true)
          ok = true
        }
      }
    }
    */
    updateNode(node)
    return true
  }

  private Void borrow(WBNode node, WBNode sibling, Bool isLeft) {
    tlist := sibling.removeAll(!isLeft)
    node.insertAll(tlist, isLeft)
    updateNode(sibling)
  }

  private Bool trySplit(BSResult path, WBNode node) {
    if (node.byteSize > bufSize || node.size >= node.maxSize) {
      //echo("splitNode=$node.id, $node.size")
      newNode := node.split(createNode)
      WBNode? parentNode
      if (path.parent != null) {
        parentNode = path.parent.node.toWBNode
        pos := path.parent.index
        if (pos == -1) { pos = 0 }
        parentNode.set(pos, node.greater, node.id)
        parentNode.insert(pos+1, newNode.greater, newNode.id)
        trySplit(path.parent, parentNode)
        updateNode(parentNode)
      } else {
        parentNode = WBNode.makeEmpty(createNode, maxKeySize, false)
        parentNode.insert(0, node.greater, node.id)
        parentNode.set(1, newNode.greater, newNode.id)
        updateNode(parentNode, true)
      }
      //echo("$node, $parentNode, $newNode")
      updateNode(newNode)
      return true
    }
    return false
  }

  Void insert(Buf key, Int address, Buf? val, Bool append := false) {
    result := search(key)
    node := result.node.toWBNode

    //Update
    if (node.leaf && (result.val != null || result.pointer != -1)) {
      if (val != null && append) {
        bufsize := result.val.size + val.size
        buf := Buf(bufsize)
        result.val.seek(0)
        buf.writeBuf(result.val)
        val.seek(0)
        buf.writeBuf(val)
        buf.flip
        val = buf
      }
      item := node.get(result.index)
      item.val = val
      item.pointer = address
      updateNode(node)
      return
    }

    node.insert(result.index, key, address, val)
    trySplit(result, node)
    updateNode(node)
  }

  Void dump() {
    root.dump(this)
  }

  Void scan(|Int ptr, Buf? val| f) {
    result := search(Buf())
    node := result.node
    while (node.leaf) {
      for (i:=0; i<node.size-1; ++i) {
        ptr := node.getPointer(i)
        val := node.getVal(i)
        f(ptr, val)
      }

      ptr := node.getPointer(node.size-1)
      if (ptr == -1) return
      node = getNode(ptr)
    }
  }
}