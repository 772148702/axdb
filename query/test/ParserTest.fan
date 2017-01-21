
class ParserTest : Test {
  static Void parse(Str sql) {
  /*
    tokenzier := Tokenizer(sql)
    TokenVal val := tokenzier.next
    while (val.kind != Token.eof) {
      echo(val)
      val = tokenzier.next
    }
    echo("======")
*/
    parser := Parser(sql)
    unit := parser.parse
    unit.dump
  }

  Void test() {
    sqls := [
      "select * FROM User where age > 25 And Name='Wilson'",
      "select * FROM User where age + 1 * 5 + 2 > 30",
      "INSERT INTO User(name, age) VALUES ('Wilson', 30)",
      "UPDATE User SET age = 23 WHERE Name = 'Wilson'",
      "DELETE FROM User WHERE Name = 'Wilson' ",
      "CREATE TABLE User(id bitint NOT NULL,name varchar(255), age int, PRIMARY KEY (id))",
      "DROP TABLE User",
    ]

    sqls.each {
      parse(it)
    }
  }
}