

class ClientTest : Test {

  Void test1() {
    c := Client(`http://localhost:8081/m1`)
    c.exeSql("CREATE TABLE User(id varchar(255) NOT NULL,name varchar(255), age int, PRIMARY KEY (id))")
    c.exeSql("INSERT INTO User(id, name, age) VALUES ('123','Wilson', 30)")
    res := c.exeSql("sql=select * FROM User where id='123'")
    echo(res)
    c.exeSql("Drop table User")
  }
}