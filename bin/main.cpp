#include <iostream>
#include <regex>
#include <string>
#include <vector>
#include "../lib/BD/BD.h"

int main() {
  MyCoolDB b;
  b.LoadTables("C:\\Users\\10a-y\\CLionProjects\\labwork-12-Ayazkins\\bin\\test.txt");
  std::cout << b.table("hello").name_ << ' ';
  for (auto x :  b.table("hello").columns_) {
	std::cout << x.Name() << ' ';
  }
  b.Upload("C:\\Users\\10a-y\\CLionProjects\\labwork-12-Ayazkins\\bin\\test2.txt");
}