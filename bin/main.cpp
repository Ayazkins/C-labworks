#include "../lib/BD/BD.h"

int main() {
  MyCoolDB b;
  b.Parse("CREATE TABLE users (id int PRIMARY KEY, name varchar, email varchar);");
  std::cout << b.tables_["users"].columns_[0].Name() << ' ';
  b.Parse("INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com');");
  b.Parse("INSERT INTO users (id, name, email) VALUES (2, Emma, 'emma@example.com');");
  b.Parse("SELECT * FROM users;");
  //b.Parse("UPDATE users SET id = 30, name = 'ayaz';");
  //b.Parse("SELECT name, email FROM users WHERE id = 2, name = Emma;");
  b.Parse("DELETE FROM users WHERE name = Emma;");
  b.Parse("SELECT * FROM users;");
  b.SaveTofIle("C:\\Users\\10a-y\\CLionProjects\\labwork-12-Ayazkins\\bin\\test.txt");
}
