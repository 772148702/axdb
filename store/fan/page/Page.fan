//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

**
** store unit
**
class Page {
  static const Int headerSize := 40
  static const Int invalidId := -1

  ** bytes num of the page
  Int pageSize

  ** size except header
  Int contentSize() { pageSize - headerSize }

  ** page address
  Int id

  ** next page of link
  Int nextPageId := invalidId

  ** backup version of this page
  Int versionPageId := invalidId

  ** content buffer
  Buf buf

  ** be changed
  Bool dirty := false

  ** not have actual store
  Bool dangling := false

  ** last modify transition id
  Int transId := -1

  new make(Int pageSize, Int id, InStream? in) {
    //this.store = store
    this.id = id
    this.pageSize = pageSize

    if (in != null) {
      code := in.readS8
      nextPageId = in.readS8
      versionPageId = in.readS8
      transId = in.readS8
      in.readS8

      buf = Buf(contentSize)
      n := in.readBuf(buf, contentSize)
      while (n != null && n < contentSize) {
        n = in.readBuf(buf, contentSize)
      }
      buf.size = contentSize

      buf.seek(0)
      code2 := buf.crc("CRC-32")
      if (code != code2) {
        throw Err("CRC Error: page:$id, $code != $code2, bufsize:$buf.size")
      }

    } else {
      buf = Buf(contentSize)
      buf.size = contentSize
      buf.seek(0)
      dangling = true
    }
  }

  new makeDup(Page other) {
    this.id = other.id
    this.dangling = other.dangling
    this.pageSize = other.pageSize

    buf = Buf(contentSize)
    buf.size = contentSize
    other.copyTo(this)
  }

  ** copy value except id
  Void copyTo(Page other) {
    other.pageSize = this.pageSize
    other.nextPageId = this.nextPageId
    other.versionPageId = this.versionPageId
    other.transId = this.transId
    //other.dirty = this.dirty
    other.buf.seek(0)
    this.buf.seek(0)
    other.buf.writeBuf(this.buf)
    this.buf.flip
  }

  override Str toStr() {
    "id=$id, dirty=$dirty, nextPId=$nextPageId, transId=$transId, dangling=$dangling, versionPId=$versionPageId, bufSize=$buf.size"
  }

  Void verifyBuf() {
    if (buf.size != contentSize) {
      throw Err("buf size error: $buf.size != $contentSize")
    }
  }

  Void write(OutStream out) {
    if (buf.size != contentSize) {
      throw Err("buf size error: $buf.size != $contentSize")
    }
    buf.seek(0)
    Int code := buf.crc("CRC-32")
    //echo("page:$id, code:$code, bufsize:$buf.size")
    out.writeI8(code)
    out.writeI8(nextPageId)
    out.writeI8(versionPageId)
    out.writeI8(transId)
    out.writeI8(0)
    out.writeBuf(buf, contentSize)
    buf.seek(0)
  }

  internal Buf fullPageBuf() {
    b := Buf(pageSize)
    write(b.out)
    b.flip
    return b
  }
}

