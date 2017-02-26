//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent
using web

class SNode {
  Uri[] list := [,]
}

class SRange {
  Int start
  Int node
}

class ShardingInfo {
  static const Int maxHash := 65535
  SRange[] ranges := [,]
  SNode[] nodes := [,]

  SNode getNode(Buf key) {
    hash := bufHash(key) % maxHash
    idx := ranges.binaryFind |v,i| {
      v.start <=> hash
    }

    if (idx < 0) {
      idx = -idx - 1
    }
    id := ranges[idx].node
    return nodes[id]
  }

  private Int bufHash(Buf key) {
    key.crc("CRC-32").abs
  }
}

class ConfigServ {

}

