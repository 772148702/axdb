//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

** File for save metadata of LogFile
class LogPosFile {
  private File path
  private Str name

  private Buf? buf

  ** version for file format
  Int version := 0

  ** flag for commit change
  Int flag := 0

  ** locally offset of file
  Int offset := 0

  ** offsetPoint global history pos
  Int beginPos := 0

  ** content length
  Int length := 0

  ** last checkPoint pos
  Int checkPoint := 0

  ** file count
  Int fileCount := 1

  ** size file per
  Int fileSize := 1024 * 1024 * 1024

  private File posFile
  private File posFile2

  new make(File dir, Str name) {
    this.path = dir
    this.name = name

    posFile = dir + `${name}-log.pos`
    posFile2 = dir + `${name}-log2.pos`
  }

  private Void read() {
    buf.seek(0)
    in := buf.in

    version = in.readS8
    flag = in.readS8
    offset = in.readS8
    beginPos = in.readS8
    length = in.readS8
    checkPoint = in.readS8
    fileCount = in.readS8
    fileSize = in.readS8
  }

  Void flushFlag() {
    buf.seek(8)
    buf.out.writeI8(flag)
    buf.out.flush
    buf.sync
  }

  private Void write() {
    buf.seek(0)
    out := buf.out
    out.writeI8(version)
    out.writeI8(flag)
    out.writeI8(offset)
    out.writeI8(beginPos)
    out.writeI8(length)
    out.writeI8(checkPoint)
    out.writeI8(fileCount)
    out.writeI8(fileSize)
  }

  override Str toStr() {
    "flag=$flag,offset=$offset,beginPos=$beginPos,length=$length,checkPoint=$checkPoint,fileCount=$fileCount,fileSize=$fileSize"
  }

  Void flush() {
    write
    buf.sync
  }

  Void close() {
    flush
    buf.close
  }

  Void openFile(Bool backup := false) {
    if (buf != null) {
      flush
      buf.close
    }

    f := backup ? posFile2 : posFile
    if (f.exists) {
      buf = f.open
      read
    } else {
      buf = f.open
      write
    }
  }

  Void open(LogFile logFile) {
    if (!posFile2.exists) {
      openFile
      return
    }

    openFile(true)

    PageMgr.log.debug("start recover:flag=$flag,fileCount=$fileCount")

    for (i := 0; i<fileCount; ++i) {
      f := logFile.getFile(i)
      f2 := logFile.getFile(i + flag)
      if (!f.exists) {
        f2.rename(f.name)
      }
      else if (f2.exists) {
        f.delete
        f2.rename(f.name)
      }
    }

    flag = 0
    flushFlag

    posFile.delete
    posFile2.rename(posFile.name)

    openFile
  }
}

**
** Append only and random read File
** can be trim from front
**
class LogFile {
  private File path
  private Str name

  private Int curFileId := -1
  private Buf? curFileBuf
  private LogPosFile posFile
  Int lastReadPos := 0 { private set }

  Bool pin := false

  private Bool dirty := true
  private Compress compress := Compress{}

  Int fileSize {
    get { posFile.fileSize }
    set { posFile.fileSize = it }
  }

  Int checkPoint {
    get { posFile.checkPoint }
    set { posFile.checkPoint = it }
  }

  internal Int fileCount() { posFile.fileCount }

  new make(File dir, Str name) {
    this.path = dir
    this.name = name
    if (!dir.exists) {
      dir.create
    }
    posFile = LogPosFile(dir, name)
  }

  Void open() {
    posFile.open(this)
    openFile(posFile.fileCount-1)
  }

  Int length() {
    posFile.length
  }

  Void writeBufObj(Buf buf) {
    buf.seek(0)
    //compress
    comBuf := compress.compress(buf)

    nbuf := Buf(comBuf.size + 4)
    nbuf.writeI4(comBuf.size)
    nbuf.writeBuf(comBuf)
    nbuf.flip

    //echo("compress:$buf.size=>$nbuf.size")

    writeBuf(nbuf)
  }

  Buf? readBufObj(Int pos) {
    try {
      if (pos >= length) return null

      //read size
      buf := Buf(4)
      n := readBuf(pos, buf, 4)
      if (n != 4) return null
      buf.flip
      size := buf.readS4

      //read buf
      buf2 := Buf(size)
      n = readBuf(pos+4, buf2, size)
      if (n != size) return null
      buf2.flip

      //update pos
      lastReadPos = pos + 4 + size

      //uncompress
      uncomBuf := compress.uncompress(buf2.in)
      return uncomBuf
    } catch (Err e) {
      e.trace
      return null
    }
  }

  Void writeBuf(Buf buf, Int n := buf.remaining) {
    seek(posFile.length, true)

    if (curFileBuf.size != curFileBuf.pos) {
      throw Err("buf is not at end: size=$curFileBuf.size != pos=$curFileBuf.pos
                 , len=${posFile.length+posFile.offset}(${posFile.length}+${posFile.offset})")
    }

    size := n
    while (n + curFileBuf.size > posFile.fileSize) {
      s := posFile.fileSize - curFileBuf.size
      curFileBuf.writeBuf(buf, s)
      n = n - s
      newFile
    }
    curFileBuf.writeBuf(buf, n)

    posFile.length += size
    dirty = true
  }

  Int? readBuf(Int pos, Buf buf, Int n) {
    ok := seek(pos)
    if (!ok) return null

    Int count := 0
    while (curFileBuf.remaining < n) {
      toRead := curFileBuf.remaining
      if (toRead == 0) {
        return count
      }
      count += curFileBuf.readBuf(buf, toRead)
      n -= toRead
      openFile(curFileId+1)
    }

    count += curFileBuf.readBuf(buf, n)
    return count
  }

  private Bool seek(Int pos, Bool increase:=false) {
    allPos := posFile.offset + pos
    localPos := allPos % posFile.fileSize
    fileId := allPos / posFile.fileSize

    if (fileId >= posFile.fileCount) {
      if (increase) {
        newFile
        return true
      }
      return false
    }

    openFile(fileId)

    if (localPos > curFileBuf.size) {
      return false
    }

    curFileBuf.seek(localPos)
    return true
  }

  private Void newFile() {
    posFile.fileCount++
    dirty = true
    flush
    openFile(posFile.fileCount-1)
  }

  private Bool openFile(Int fileId) {
    if (curFileId != fileId) {
      flush
      curFileBuf?.close
      file := getFile(fileId)
      curFileBuf = file.open
      curFileId = fileId
    }
    return true
  }

  internal File getFile(Int id) {
    path + `${name}-${id}.log`
  }

  Bool removeFrom(Int pos) {
    dsize := posFile.length - pos
    if (dsize <= 0) return false

    posFile.length -= dsize
    files := dsize / posFile.fileSize
    posFile.fileCount -= files
    return true
  }

  Bool trim(Int pos) {
    if (pin) return false
    PageMgr.log.debug("trim:$pos")

    posFile.offset += pos
    posFile.beginPos += pos
    posFile.length -= pos
    posFile.checkPoint -= pos
    if (posFile.checkPoint < 0) {
      posFile.checkPoint = 0
    }
    posFile.flush

    beginFileId := posFile.offset / posFile.fileSize
    if (beginFileId == 0) return false

    //open backup file
    posFile.openFile(true)

    offset := beginFileId * posFile.fileSize
    posFile.offset -= offset
    posFile.fileCount -= beginFileId
    posFile.flag = -1
    posFile.flush

    //commit
    posFile.flag = beginFileId
    posFile.flushFlag

    //open and recover
    posFile.open(this)

    return true
  }

  Void flush() {
    if (!dirty) return

    PageMgr.log.debug("flush log file")

    curFileBuf?.sync
    posFile.flush
    dirty = false
  }

  Void close() {
    flush
    posFile.close
    curFileBuf?.close
  }

  Void dump() {
    echo(posFile)
  }
}


