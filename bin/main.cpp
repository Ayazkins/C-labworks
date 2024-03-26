//#include "../lib/BD/BD.h"
//
//int main() {
//
//  MyCoolDB b;
//  b.Parse("CREATE TABLE Users (id INT, UserName VARCHAR);");
//  b.Parse("INSERT INTO Users (id, UserName) VALUES (4, 'John Doe');");
//  b.Parse("INSERT INTO Users (id, UserName) VALUES (2, 'Jane Smith');");
//  b.Parse("INSERT INTO Users (id, UserName) VALUES (3, 'Bob Johnson');");
//
//  b.Parse("CREATE TABLE Songs (SongId INT, SongName VARCHAR);");
//
//  b.Parse("INSERT INTO Songs (SongId, SongName) VALUES (3, 'Sleep');");
//  b.Parse("INSERT INTO Songs (SongId, SongName) VALUES (1, 'Relax');");
//  b.Parse("INSERT INTO Songs (SongId, SongName) VALUES (3, 'Chill');");
//  b.Parse("INSERT INTO Songs (SongId, SongName) VALUES (4, 'Have fun');");
//  b.Parse("SELECT * FROM Users INNER JOIN Songs ON Users.id = Songs.SongId;");
//  std::cout << "LEFT JOIN TEST\n";
//  b.Parse("SELECT * FROM Users LEFT JOIN Songs ON Users.id = Songs.SongId;");
//  std::cout << "RIGHT JOIN TEST\n";
//  b.Parse("SELECT * FROM Users RIGHT JOIN Songs ON Users.id = Songs.SongId;");
//
//}


#include <iostream>
#include <vector>
#include <string_view>
#include <fstream>
#include <cstring>
#include <cstdint>
#include <codecvt>
#include <string>
#include <locale>

std::u16string convertBigEndianToLittleEndian(const std::u16string &bigEndianStr) {
  std::vector<uint8_t> bytes(bigEndianStr.size() * 2);
  for (size_t i = 0; i < bigEndianStr.size(); i++) {
	uint16_t value = static_cast<uint16_t>(bigEndianStr[i]);
	bytes[i * 2] = static_cast<uint8_t>(value >> 8);
	bytes[i * 2 + 1] = static_cast<uint8_t>(value);
  }
  for (size_t i = 0; i < bytes.size() / 2; i++) {
	uint8_t temp = bytes[i * 2];
	bytes[i * 2] = bytes[i * 2 + 1];
	bytes[i * 2 + 1] = temp;
  }
  std::u16string littleEndianStr;
  for (size_t i = 0; i < bytes.size() / 2; i++) {
	uint16_t value = static_cast<uint16_t>((bytes[i * 2] << 8) | bytes[i * 2 + 1]);
	littleEndianStr.push_back(static_cast<char16_t>(value));
  }
  return littleEndianStr;
}

std::string ReadUtf16(std::ifstream &file, const int size) {
  char bom[2];
  file.read(bom, 2);
  std::u16string u16((size / 2) + 1, '\0');
  file.read((char *)&u16[0], size);
  if (static_cast<int>(bom[0]) == -2) {
	u16 = convertBigEndianToLittleEndian(u16);
  }
  std::string utf8 = std::wstring_convert <
  std::codecvt_utf8_utf16 < char16_t > , char16_t > {}.to_bytes(u16);
  return utf8;
}

std::string ReadUtf16WithoutBom(std::ifstream &file, const int size) {
  std::u16string u16((size / 2) + 1, '\0');
  file.read((char *)&u16[0], size);
  u16 = convertBigEndianToLittleEndian(u16);
  std::string utf8 = std::wstring_convert <
  std::codecvt_utf8_utf16 < char16_t > , char16_t > {}.to_bytes(u16);
  return utf8;
}

std::string ReadFrameData(std::ifstream& file, const int size, const int code) {
  std::string frameData(size, '\0');
  if (code == 0x01) {
	frameData = ReadUtf16(file, size);
  } else if (code == 0x02) {
	frameData = ReadUtf16WithoutBom(file, size);
  } else {
	file.read(&frameData[0], size);
  }
  return frameData;
}

int main() {
  std::ifstream file("C:\\Users\\10a-y\\CLionProjects\\labwork-12-Ayazkins\\testUtf.txt");
  std::cout << ReadUtf16(file, 22);
}