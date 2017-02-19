#! /usr/bin/env fan
//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

using build

**
** Build: util
**
class Build : BuildPod
{
  new make()
  {
    podName  = "axdbCluster"
    summary  = "distributed cluster"
    depends  = ["sys 1.0", "concurrent 1.0", "web 1.0", "util 1.0", "axdbStore 1.0"]
    srcDirs  = [`fan/`, `fan/raft/`]
  }
}

