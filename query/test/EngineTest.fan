using axdbStore

class EngineTest : Test {

  File path := File(`data/`)
  Str name := "data"

  override Void setup() {
    path.delete
  }

  Void test() {
    engine := Engine(path, name)
    executor := Executor(engine)

    res := executor.exeSql("BEGIN TRANSACTION")
    echo(res)

    res = executor.exeSql("CREATE TABLE User(id varchar(255) NOT NULL,name varchar(255), age int, PRIMARY KEY (id))")
    echo(res)

    res = executor.exeSql("INSERT INTO User(id, name, age) VALUES ('123','Wilson', 30)")
    echo(res)

    res = executor.exeSql("select * FROM User where id='123'")
    echo(res)
  }
}