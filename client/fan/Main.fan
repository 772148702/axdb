//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using concurrent
using web

class Main {
  Client? client

  Void main(Str[] arg) {
    Uri host := arg[0].toUri
    client = Client(host)

    echo("axdb")
    while (true) {
      echo(">>")

      line := Env.cur.in.readLine
      if (line == null || line == "exit") break

      try {
        res := client.exeSql(line)
        echo(res)
      }
      catch (Err e) {
        e.trace
      }
    }
  }
}


