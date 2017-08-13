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

    Obj? res
    res = executor.exeSql("BEGIN TRANSACTION")
    echo(res)

    res = executor.exeSql("CREATE TABLE User(id varchar(255) NOT NULL,name varchar(255), age int, PRIMARY KEY (id))")
    echo(res)

    res = executor.exeSql("INSERT INTO User(id, name, age) VALUES ('123','Wilson', 30)")
    echo(res)

    res = executor.exeSql("select * FROM User where id='123'")
    echo(res)

    res = executor.exeSql("select * FROM User where id='123'")
    echo(res)

    res = executor.exeSql("COMMIT TRANSACTION")
    echo(res)
  }

  private Void exeSql(Str sql) {
    engine := Engine(path, name)
    res := engine.exeSql(sql)
    echo(res)
    engine.close
  }

  Void testSql() {
    exeSql("CREATE TABLE User(id varchar(255) NOT NULL,name varchar(255), age int, PRIMARY KEY (id))")
    exeSql("INSERT INTO User(id, name, age) VALUES ('123','Wilson', 30)")
    exeSql("select * FROM User where id='123'")
    exeSql("select * FROM User where id='123'")
  }

  Void testData() {
    engine := Engine(path, name)
    executor := Executor(engine)

    executor.exeSql("CREATE TABLE User(id varchar(255) NOT NULL,name varchar(255), age int, PRIMARY KEY (id))")
    300.times {
      executor.exeSql("INSERT INTO User(id, name, age) VALUES ('$it','Wilson', 30)")
    }
    res := executor.exeSql("select * FROM User where id='99'")
    echo(res)
    res = executor.exeSql("select * FROM User where id='299'")
    echo(res)
  }
}