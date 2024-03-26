#include "../lib/BD/BD.h"
#include "../lib/BD/BD.cpp"
#include "gtest/gtest.h"

TEST(SQL_TEST, CREATE) {
  MyCoolDB B;
  B.Parse("CREATE TABLE users (id int PRIMARY KEY, name varchar, email varchar);");
  EXPECT_EQ(B.tables_["users"].columns_[0].Name(), "id");
  EXPECT_EQ(B.tables_["users"].columns_[0].Type(), Type::INT);
  EXPECT_EQ(B.tables_["users"].columns_[0].Key(), "PRIMARY KEY");
  EXPECT_EQ(B.tables_["users"].columns_[1].Name(), "name");
  EXPECT_EQ(B.tables_["users"].columns_[1].Type(), Type::VARCHAR);
  EXPECT_EQ(B.tables_["users"].columns_[1].Key(), "NOT KEY");
  B.Parse("SELECT * FROM users;");
}

TEST(SQL_TEST, INSERT) {
  MyCoolDB B;
  B.Parse("CREATE TABLE users (id int PRIMARY KEY, name varchar, email varchar);");
  B.Parse("INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com');");
  B.Parse("INSERT INTO users (id, name, email) VALUES (2, 'Emma', 'emma@example.com');");
  std::vector<std::vector<std::string>>
	  a = {{"1", "'John'", "'john@example.com'"}, {"2", "'Emma'", "'emma@example.com'"}};
  for (int i = 0; i < 2; ++i) {
	int k = 0;
	for (auto x : {"id", "name", "email"}) {
	  EXPECT_EQ(B.tables_["users"].data_[i].data[x], a[i][k]);
	  k++;
	}
  }
}

TEST(SQL_TEST, UPDATE) {
  MyCoolDB B;
  B.Parse("CREATE TABLE users (id int PRIMARY KEY, name varchar, email varchar);");
  B.Parse("INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com');");
  B.Parse("INSERT INTO users (id, name, email) VALUES (2, 'Emma', 'emma@example.com');");
  B.Parse("UPDATE users SET id = 30, name = 'ayaz' WHERE id = 2;");
  std::vector<std::vector<std::string>>
	  a = {{"1", "'John'", "'john@example.com'"}, {"30", "'ayaz'", "'emma@example.com'"}};
  for (int i = 0; i < 2; ++i) {
	int k = 0;
	for (auto x : {"id", "name", "email"}) {
	  EXPECT_EQ(B.tables_["users"].data_[i].data[x], a[i][k]);
	  k++;
	}
  }
  B.Parse("SELECT * FROM users;");
}

TEST(SQL_TEST, Primary_key) {
  MyCoolDB B;
  B.Parse("CREATE TABLE users (id int PRIMARY KEY, name varchar, email varchar);");
  B.Parse("INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com');");
  try {
	B.Parse("INSERT INTO users (id, name, email) VALUES (1, 'Emma', 'emma@example.com');");
  }
  catch (const std::exception& e) {
	std::cout << e.what() << ' ';
  }
  B.Parse("INSERT INTO users (id, name, email) VALUES (2, 'Emma', 'emma@example.com');");
  B.Parse("SELECT * FROM users;");
}

TEST(SQL_TEST, NULL_TEST) {
  MyCoolDB B;
  B.Parse("CREATE TABLE users (id int NOT NULL PRIMARY KEY, name varchar, email varchar);");
  B.Parse("INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com');");
  try {
	B.Parse("INSERT INTO users (name, email) VALUES ('Emma', 'emma@example.com');");
  }
  catch (const std::exception& e) {
	std::cout << e.what() << ' ';
  }
  B.Parse("INSERT INTO users (id, name, email) VALUES (2, 'John', 'john@example.com');");
}

TEST(SQL_TEST, SELECT) {
  MyCoolDB B;
  B.Parse("CREATE TABLE users (id int PRIMARY KEY, name varchar, email varchar);");
  B.Parse("INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com');");
  B.Parse("INSERT INTO users (id, name, email) VALUES (2, 'Emma', 'emma@example.com');");
  B.Parse("SELECT name, email FROM users WHERE id = 1 OR name = 'Emma';");
}

TEST(SQL_TEST, SAVE_LOAD) {
  MyCoolDB B;
  B.Parse("CREATE TABLE users (id int PRIMARY KEY, name varchar, email varchar);");
  B.Parse("INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com');");
  B.Parse("INSERT INTO users (id, name, email) VALUES (2, 'Emma', 'emma@example.com');");
  B.SaveTofIle("C:\\Users\\10a-y\\CLionProjects\\labwork-12-Ayazkins\\tests\\testing.txt");
  B.Parse("DROP TABLE users;");
  B.LoadTables("C:\\Users\\10a-y\\CLionProjects\\labwork-12-Ayazkins\\tests\\testing.txt");
  B.Parse("SELECT * FROM users;");
}

TEST(SQL_TEST, JOIN) {
  MyCoolDB b;
  b.Parse("CREATE TABLE Users (id INT, UserName VARCHAR);");
  b.Parse("INSERT INTO Users (id, UserName) VALUES (4, 'John Doe');");
  b.Parse("INSERT INTO Users (id, UserName) VALUES (2, 'Jane Smith');");
  b.Parse("INSERT INTO Users (id, UserName) VALUES (3, 'Bob Johnson');");

  b.Parse("CREATE TABLE Songs (SongId INT, SongName VARCHAR);");

  b.Parse("INSERT INTO Songs (SongId, SongName) VALUES (3, 'Sleep');");
  b.Parse("INSERT INTO Songs (SongId, SongName) VALUES (1, 'Relax');");
  b.Parse("INSERT INTO Songs (SongId, SongName) VALUES (3, 'Chill');");
  b.Parse("INSERT INTO Songs (SongId, SongName) VALUES (4, 'Have fun');");
  b.Parse("SELECT * FROM Users INNER JOIN Songs ON Users.id = Songs.SongId;");
  std::cout << "LEFT JOIN TEST\n";
  b.Parse("SELECT * FROM Users LEFT JOIN Songs ON Users.id = Songs.SongId;");
  std::cout << "RIGHT JOIN TEST\n";
  b.Parse("SELECT * FROM Users RIGHT JOIN Songs ON Users.id = Songs.SongId;");
}

TEST(SQL_TEST, WHERE) {
  MyCoolDB B;
  B.Parse("CREATE TABLE users (id int PRIMARY KEY, name varchar, email varchar);");
  B.Parse("INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com');");
  B.Parse("INSERT INTO users (name, email) VALUES ('Emma', 'emma@example.com');");
  B.Parse("SELECT * FROM users WHERE id IS NOT NULL;");
  std::cout << '\n';
  B.Parse("SELECT * FROM users WHERE id IS NULL;");
  std::cout << '\n';
  B.Parse("SELECT * FROM users WHERE id IS NULL OR id = 1;");
  std::cout << '\n';
}

TEST(SQL_TEST, DELETE) {
  MyCoolDB B;
  B.Parse("CREATE TABLE users (id int PRIMARY KEY, name varchar, email varchar);");
  B.Parse("INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com');");
  B.Parse("INSERT INTO users (id, name, email) VALUES (2, 'Emma', 'emma@example.com');");
  B.Parse("DELETE FROM users WHERE name = 'Emma';");
  B.Parse("SELECT * FROM users;");
}
