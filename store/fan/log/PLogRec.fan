//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

enum class PLogType {
  beginTrans,
  prepareTrans,
  commitTrans,
  abortTrans,

  startCheckPoint,
  endCheckPoint,

  updatePage,
  freePage,
  createPage
}

class PLogRec {
  Int transId

  ** previous is distance of previous log rec of the trans
  Int previous := -1

  PLogType type

  Str tableSpace := ""

  new make(|This|? f := null) {
    f?.call(this)
  }

  static PLogRec readRec(InStream in) {
    ptype := PLogType.vals[in.read]
    switch (ptype) {
      case PLogType.beginTrans:
      case PLogType.prepareTrans:
      case PLogType.commitTrans:
      case PLogType.abortTrans:
        return PLogRec.read(ptype, in)
      case PLogType.startCheckPoint:
      case PLogType.endCheckPoint:
        return CheckLogRec.read(ptype, in)
      case PLogType.updatePage:
        return PageLogRec.read(ptype, in)
      case PLogType.freePage:
        return FreePageLogRec.read(ptype, in)
      case PLogType.createPage:
        return CreatePageLogRec.read(ptype, in)
    }
    throw Err("unknow type $ptype")
  }

  virtual Void write(OutStream out) {
    out.write(type.ordinal)
    out.writeI8(transId)
    out.writeI8(previous)
    out.writeUtf(tableSpace)
  }

  new read(PLogType t, InStream in) {
    type = t
    transId = in.readS8
    previous = in.readS8
    tableSpace = in.readUtf
  }

  override Str toStr() {
    "$type, trans=$transId, prev=$previous, ts=$tableSpace"
  }
}

class PageLogRec : PLogRec {
  Int pageId := Page.invalidId
  Buf newData
  Buf? oldData

  new make(|This| f) : super.make(f) {
  }

  override Void write(OutStream out) {
    super.write(out)
    out.writeI8(pageId)
    out.writeI4(newData.size)
    out.writeBuf(newData)

    if (oldData != null) {
      out.writeI4(oldData.size)
      out.writeBuf(oldData)
    } else {
      out.writeI4(0)
    }
  }

  new read(PLogType t, InStream in) : super.read(t, in) {
    //super.read(in)
    pageId = in.readS8
    size := in.readS4
    newData = Buf(size)
    in.readBuf(newData, size)
    newData.flip

    size2 := in.readS4
    if (size2 > 0) {
      oldData = Buf(size2)
      in.readBuf(oldData, size2)
      oldData.flip
    }
  }

  override Str toStr() {
    s := super.toStr
    return "$s, pageId=$pageId"
  }
}
/*
class StoreLogRec : PLogRec {
  Int oldFreePage
  Int newFreePage
  Int oldPageCount
  Int newPageCount

  new make(|This| f) : super.make(f) {
  }

  override Void write(OutStream out) {
    super.write(out)
    out.writeI8(oldFreePage)
    out.writeI8(newFreePage)
    out.writeI8(oldPageCount)
    out.writeI8(newPageCount)
  }

  new read(PLogType t, InStream in) : super.read(t, in) {
    //super.read(in)
    oldFreePage = in.readS8
    newFreePage = in.readS8
    oldPageCount = in.readS8
    newPageCount = in.readS8
  }

  override Str toStr() {
    s := super.toStr
    return "$s, $oldFreePage => $newFreePage $oldPageCount => $newPageCount"
  }
}
*/
class FreePageLogRec : PLogRec {
  Int firstPage
  Int lastPage
  Int newFreePage
  Int newPageCount

  new make(|This| f) : super.make(f) {
  }

  override Void write(OutStream out) {
    super.write(out)
    out.writeI8(firstPage)
    out.writeI8(lastPage)
    out.writeI8(newFreePage)
    out.writeI8(newPageCount)
  }

  new read(PLogType t, InStream in) : super.read(t, in) {
    //super.read(in)
    firstPage = in.readS8
    lastPage = in.readS8
    newFreePage = in.readS8
    newPageCount = in.readS8
  }

  override Str toStr() {
    s := super.toStr
    return "$s, firstPage=$firstPage lastPage=$lastPage"
  }
}

class CreatePageLogRec : PLogRec {
  Int pageId
  Int newFreePage
  Int newPageCount

  new make(|This| f) : super.make(f) {
  }

  override Void write(OutStream out) {
    super.write(out)
    out.writeI8(pageId)
    out.writeI8(newFreePage)
    out.writeI8(newPageCount)
  }

  new read(PLogType t, InStream in) : super.read(t, in) {
    //super.read(in)
    pageId = in.readS8
    newFreePage = in.readS8
    newPageCount = in.readS8
  }

  override Str toStr() {
    s := super.toStr
    return "$s, pageId:$pageId"
  }
}

class CheckLogRec : PLogRec {
  ** current unfinished trans list
  Int[] transList

  **last log write pos of each trans
  Int[] transPos

  new make(|This| f) : super.make(f) {
  }

  override Void write(OutStream out) {
    super.write(out)
    out.writeI4(transList.size)
    transList.each {
      out.writeI8(it)
    }
    transPos.each {
      out.writeI8(it)
    }
  }

  new read(PLogType t, InStream in) : super.read(t, in) {
    //super.read(in)
    size := in.readS4
    transList = [,] { capacity = size }
    transPos = [,] { capacity = size }
    size.times {
      transList.add(in.readS8)
    }
    size.times {
      transPos.add(in.readS8)
    }
  }

  override Str toStr() {
    s := super.toStr
    return "$s, transList:$transList"
  }
}

