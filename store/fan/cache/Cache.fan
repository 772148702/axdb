//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

class CacheItem : LinkedElem {
  Obj? key
  Int cacheCount := 0
}

class LruCache {
  private Obj:CacheItem map := Obj:CacheItem[:]
  private LinkedList list := LinkedList()

  private Int max
  |Obj|? onRemoveItem
  |Obj->Bool|? canRemoveItem

  private Int maxCount := 1024

  new make(Int size) { max = size }

  CacheItem? getItem(Obj key) {
    item := map[key]
    if (item != null) {
      update(item)
      return item
    }
    return null
  }

  Void each(|Obj| f) {
    item := list.first
    while (item != list.end) {
      if (item.val != null) {
        f(item.val)
      }
      item = item.next
    }
  }

  Obj? get(Obj key) {
    item := map[key]
    if (item != null) {
      update(item)
      return item.val
    }
    return null
  }

  virtual CacheItem newItem() {
    CacheItem()
  }

  private Void update(CacheItem item) {
    if (item.previous != null) {
      if (item.cacheCount < maxCount) {
        item.cacheCount++
      }
      item.remove
    }

    list.insertBefore(item)
  }

  private CacheItem? clean() {
    if (map.size <= max) return null
    CacheItem item := list.last
    while (item != list.end) {
      pre := item.previous
      if (item.cacheCount > 0) {
        item.cacheCount = item.cacheCount - 1
        item.remove
        list.insertBefore(item)
      } else {
        canRemove := true
        if (canRemoveItem != null && item.val != null) {
          canRemove = canRemoveItem(item.val)
        }
        if (canRemove) {
          map.remove(item.key)
          item.remove
          onReomove(item)
          item.cacheCount = 0
          return item
        }
      }
      item = pre
    }
    return null
  }

  Void clear() {
    item := list.last
    while (item != list.end) {
      pre := item.previous
      item.remove
      onReomove(item)
      item = pre
    }
    list.clear
    map.clear
  }

  Void set(Obj key, Obj? val) {
    item := map[key]
    if (item == null) {
      item = clean()
      if (item == null) {
        item = newItem
      }
    }

    item.val = val
    item.key = key

    update(item)
    map[key] = item
  }

  protected virtual Void onReomove(CacheItem e) {
    if (onRemoveItem != null && e.val != null) {
      onRemoveItem(e.val)
    }
  }

}