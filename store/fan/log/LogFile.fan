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

  ** flag for commit change, num of fold
  private Int foldNum := 0

  ** pos of first record in files(will fold to first file by trim)
  Int offset := 0

  ** as sanme offset but global history pos
  Int globalPos := 0

  ** content length
  Int length := 0

  ** last checkPoint pos(base of offset)
  Int checkPoint := 0

  ** file count
  Int fileCount := 1

  ** size file per
  Int fileSize := 10 * 1024 * 1024

  Int userData := 0

  Int unused := 0

  private File posFile
  private File tempFile

  new make(File dir, Str name) {
    this.path = dir
    this.name = name

    posFile = path + `${name}-log.pos`
    tempFile = path + `${name}-log2.pos`
  }

  private Int checkCode() {
    Int c := version
    c = c * 31 + foldNum
    c = c * 31 + offset
    c = c * 31 + globalPos
    c = c * 31 + length
    c = c * 31 + checkPoint
    c = c * 31 + fileCount
    c = c * 31 + fileSize
    c = c * 31 + userData
    c = c * 31 + unused
    return c
  }

  private Void read() {
    buf.seek(0)
    in := buf.in

    version = in.readS8
    foldNum = in.readS8
    offset = in.readS8
    globalPos = in.readS8
    length = in.readS8
    checkPoint = in.readS8
    fileCount = in.readS8
    fileSize = in.readS8
    userData = in.readS8
    unused = in.readS8
    code := in.readS8
    if (code != checkCode) {
      throw Err("check code error")
    }
  }

  private Void write() {
    buf.seek(0)
    out := buf.out
    out.writeI8(version)
    out.writeI8(foldNum)
    out.writeI8(offset)
    out.writeI8(globalPos)
    out.writeI8(length)
    out.writeI8(checkPoint)
    out.writeI8(fileCount)
    out.writeI8(fileSize)
    out.writeI8(userData)
    out.writeI8(unused)
    out.writeI8(checkCode)
  }

  override Str toStr() {
    "foldNum=$foldNum,offset=$offset,globalPos=$globalPos,length=$length,checkPoint=$checkPoint,fileCount=$fileCount,fileSize=$fileSize"
  }

  Void flush() {
    write
    buf.sync
  }

  Void close() {
    flush
    buf.close
    buf = null
  }

  private Void openFile(Bool backup := false) {
    if (buf != null) {
      flush
      buf.close
    }

    f := backup ? tempFile : posFile
    if (f.exists) {
      buf = f.open
      read
    } else {
      buf = f.open
      write
    }
  }

  Void fold(LogFile logFile) {
    beginFileId := this.offset / this.fileSize
    if (beginFileId == 0) return false

    //open backup file
    this.openFile(true)

    offset := beginFileId * this.fileSize
    this.offset -= offset
    this.fileCount -= beginFileId
    this.foldNum = beginFileId
    this.flush

    //open and recover
    doFold(logFile)
    openFile
  }

  private Void doFold(LogFile logFile) {
    if (foldNum > 0) {
      LogFile.log.debug("start fold:foldNum=$foldNum,fileCount=$fileCount")

      for (i := 0; i<fileCount; ++i) {
        f := logFile.getFile(i)
        f2 := logFile.getFile(i + foldNum)
        if (!f.exists) {
          f2.rename(f.name)
        }
        else if (f2.exists) {
          f.delete
          f2.rename(f.name)
        }
      }

      foldNum = 0
      flush

      close
      posFile.delete
      tempFile.rename(posFile.name)
    }
    else if (foldNum < 0) {
      close
      tempFile.delete
    } else {
      close
      posFile.delete
      tempFile.rename(posFile.name)
    }
  }

  Void open(LogFile logFile) {
    if (!tempFile.exists) {
      openFile
      return
    }

    openFile(true)
    doFold(logFile)
    openFile
  }
}

**
** Append only and random read File
** Donot save the position that will be changed by trim operate
**
class LogFile {
  private File path
  private Str name

  private Int curFileId := -1
  private Buf? curFileBuf
  private LogPosFile posFile
  Int lastReadPos := 0 { private set }

  ** avoid trim
  Bool pin := false

  private Bool dirty := true
  private Compress compress := Compress{}

  internal static const Log log := Log.get("axdbStore.LogFile")

  Int fileSize {
    get { posFile.fileSize }
    set { posFile.fileSize = it }
  }

  Int checkPoint {
    get { posFile.checkPoint }
    set { posFile.checkPoint = it }
  }

  Int userData {
    get { posFile.userData }
    set { posFile.userData = it }
  }

  internal Int fileCount() { posFile.fileCount }

  new make(File dir, Str name) {
    this.path = dir
    this.name = name
    if (!path.exists) {
      path.create
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

  **
  ** write compressed obj to last
  **
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

  **
  ** read compressed obj from position and update lastReadPos
  **
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

  **
  ** append to last
  **
  Void writeBuf(Buf buf, Int n := buf.remaining) {
    seek(posFile.length, true)
    log.debug("write log $buf, $n")

    //if (curFileBuf.size != curFileBuf.pos) {
    //  throw Err("buf is not at end: size=$curFileBuf.size != pos=$curFileBuf.pos
    //             , len=${posFile.length+posFile.offset}(${posFile.length}+${posFile.offset})")
    //}

    size := n
    while (n + curFileBuf.pos > posFile.fileSize) {
      s := posFile.fileSize - curFileBuf.pos
      curFileBuf.writeBuf(buf, s)
      n = n - s
      newFile
    }
    curFileBuf.writeBuf(buf, n)

    posFile.length += size
    dirty = true
  }

  **
  ** read from postion
  **
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
      if (curFileId != -1) {
        flush
      }
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

  ** remove range[pos..-1]
  Bool removeFrom(Int pos) {
    dsize := posFile.length - pos
    if (dsize <= 0) return false
    log.debug("remove from $pos")

    posFile.length -= dsize
    files := dsize / posFile.fileSize
    posFile.fileCount -= files
    return true
  }

  **
  ** remvoe before pos and move origin to pos
  ** pos is base of offset
  **
  Bool trim(Int pos) {
    if (pin) return false
    log.debug("trim:$pos, $posFile")

    posFile.offset += pos
    posFile.globalPos += pos
    posFile.length -= pos
    posFile.checkPoint -= pos
    if (posFile.checkPoint < 0) {
      posFile.checkPoint = 0
    }
    posFile.flush

    posFile.fold(this)

    return true
  }

  Void flush() {
    if (!dirty) return

    log.debug("$path $name flush log file")

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


