//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using axdbStore

class Engine {
  StoreClient store
  TableMeta tableMeta := TableMeta()

  new make(File path, Str name) {
    store = StoreClient(path, name)
  }
}