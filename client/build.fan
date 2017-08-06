#! /usr/bin/env fan
//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-8-16  Jed Young  Creation
//

using build

**
** Build: util
**
class Build : BuildPod
{
  new make()
  {
    podName  = "axdbClient"
    summary  = "axdb client api"
    depends  = ["sys 1.0", "concurrent 1.0", "web 1.0", "util 1.0"]
    srcDirs  = [`fan/`, `test/`]
  }
}

