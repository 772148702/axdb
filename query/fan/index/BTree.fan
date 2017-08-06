//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

abstract class BTree {

  Int maxKeySize := 1024
  Int bufSize := 4096 + ((maxKeySize+1) * 10)
  RBNode? root { private set }

  new make() {
  }

  This initRoot(Int transId, Int rootId := -1) {
    if (rootId == -1) {
      temp := WBNode.makeEmpty(createNode(transId), maxKeySize, true)
      root = RBNode(temp.id, temp.toBuf.toImmutable)
      updateNode(transId, temp)
    }
    else {
      root = getNode(transId, rootId)
    }
    return this
  }

  abstract Buf readNode(Int transId, Int id)
  abstract Int createNode(Int transId)
  abstract Void updateBuf(Int transId, Int id, Buf buf)

  virtual RBNode getNode(Int transId, Int id) {
    buf := readNode(transId, id)
    node := RBNode(id, buf)
    return node
  }

  private Void updateNode(Int transId, WBNode node, Bool asRoot := false) {
    ibuf := node.toBuf.toImmutable
    updateBuf(transId, node.id, ibuf)
    if (asRoot || node.id == root.id) {
      root = RBNode(node.id, ibuf)
    }
  }

  BSResult search(Int transId, Buf key) {
    node := root
    BSResult? result

    while (true) {
      tresult := node.search(key)
      tresult.parent = result
      result = tresult

      if (node.leaf) return result
      if (result.pointer == -1) return result
      node = getNode(transId, result.pointer)
    }

    return result
  }

  Bool remove(Int transId, Buf key) {
    result := search(transId, key)
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
    updateNode(transId, node)
    return true
  }

  private Void borrow(Int transId, WBNode node, WBNode sibling, Bool isLeft) {
    tlist := sibling.removeAll(!isLeft)
    node.insertAll(tlist, isLeft)
    updateNode(transId, sibling)
  }

  private Bool trySplit(Int transId, BSResult path, WBNode node) {
    if (node.byteSize > bufSize || node.size >= node.maxSize) {
      //echo("splitNode=$node.id, $node.size")
      newNode := node.split(createNode(transId))
      WBNode? parentNode
      if (path.parent != null) {
        parentNode = path.parent.node.toWBNode
        pos := path.parent.index
        if (pos == -1) { pos = 0 }
        parentNode.set(pos, node.greater, node.id)
        parentNode.insert(pos+1, newNode.greater, newNode.id)
        trySplit(transId, path.parent, parentNode)
        updateNode(transId, parentNode)
      } else {
        parentNode = WBNode.makeEmpty(createNode(transId), maxKeySize, false)
        parentNode.insert(0, node.greater, node.id)
        parentNode.set(1, newNode.greater, newNode.id)
        updateNode(transId, parentNode, true)
      }
      //echo("$node, $parentNode, $newNode")
      updateNode(transId, newNode)
      return true
    }
    return false
  }

  Void insert(Int transId, Buf key, Int address, Buf? val, Bool append := false) {
    result := search(transId, key)
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
      updateNode(transId, node)
      return
    }

    node.insert(result.index, key, address, val)
    trySplit(transId, result, node)
    updateNode(transId, node)
  }

  Void dump(Int transId) {
    root.dump(transId, this)
  }

  Void visitNode(Int transId, |Int nodeId| f, RBNode node := root) {
    if (node.leaf) {
      f(node.id)
      return
    }
    for (i:=0; i<node.size-1; ++i) {
      ptr := node.getPointer(i)
      node = getNode(transId, ptr)
      visitNode(transId, f, node)
    }
    f(node.id)
  }

  Void scan(Int transId, |Int ptr, Buf? val| f) {
    result := search(transId, Buf())
    node := result.node
    while (node.leaf) {
      for (i:=0; i<node.size-1; ++i) {
        ptr := node.getPointer(i)
        val := node.getVal(i)
        f(ptr, val)
      }

      ptr := node.getPointer(node.size-1)
      if (ptr == -1) return
      node = getNode(transId, ptr)
    }
  }
}

class BTreeIterator {
  private BTree tree
  private RBNode node
  private Int pos
  private Int transId

  new make(BTree t, Int transId) {
    tree = t
    node = tree.root
    pos = -1
    this.transId = transId
  }

  Bool more() { pos != -2 }

  Buf? next() {
    if (pos == -2) return null
    if (pos < (node.size-1)-1) {
      ++pos
      return node.getVal(pos)
    }

    ptr := node.getPointer(node.size-1)
    if (ptr == -1) {
      pos = -2
      return null
    }
    node = tree.getNode(transId, ptr)
    pos = 0
    return node.getVal(pos)
  }
}

