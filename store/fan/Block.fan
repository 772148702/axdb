//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

** Data element
const class Block {
  ** first page id
  const Int id

  ** data buffer
  const Buf buf

  ** last modify transaction id
  const Int transId := -1

  const Int versionId := -1

  ** history version data
  const Block? version := null

  private new makeDup(Block other, |This| f) {
    this.id = other.id
    this.buf = other.buf
    this.transId = other.transId
    this.versionId = other.versionId
    this.version = other.version
    f(this)
  }

  This dupWith(|This| f) {
    Block.makeDup(this, f)
  }

  new make(Int id, Buf buf, Int transId, Int versionId) {
    this.id = id
    this.buf = buf
    this.transId = transId
    this.versionId = versionId
  }

  override Str toStr() {
    "id=$id, transId=$transId, buf=$buf, version=$version"
  }
}


