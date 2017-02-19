//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using web

const class RefMod : WebMod {
  const Type type

  new make(Type type) {
    this.type = type
  }

  private Obj getArg(Str:Str querys, Param param) {
    val := querys[param.name]
    if (param.type != Str#) {
      m := param.type.method("fromStr")
      v := m.call(val)
      return v
    }
    return val
  }

  protected virtual Void doService(Bool post) {
    //get method
    paths := req.modRel.path
    name := paths[0]
    Str? ctorName := null
    if (paths.size>1) ctorName = paths[1]

    method := type.method(name)
    if (!method.isPublic) {
      res.sendErr(501)
      return
    }

    //get args
    args := [,]
    querys := post? req.form : req.uri.query
    method.params.each {
      val := getArg(querys, it)
      args.add(val)
    }

    //call
    try {
      obj := type.make([ctorName])
      r := obj.trap(name, args)

      //send result
      if (r != null) {
        res.headers["Content-Type"] = "text/plain; charset=utf-8"
        res.out.print(r)
      }
    } catch (Err e) {
      e.trace
      res.sendErr(501)
    }
  }

  override Void onGet() {
    doService(false)
  }

  override Void onPost() {
    doService(true)
  }
}