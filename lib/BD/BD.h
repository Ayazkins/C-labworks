#pragma once
#include <iostream>
#include <vector>
#include <unordered_map>
#include <fstream>
#include <regex>

enum class Type {
  BOOL,
  INT,
  FLOAT,
  DOUBLE,
  VARCHAR
};

class Columns {
 private:
  std::string name_;
  Type type_;
  std::string key_;
 public:
  Columns(const std::string &name, const std::string &type, const std::string &key = "NOT_KEY")
	  : name_(name) {
	key_ = key.substr(0, key.find(' ')) + "_" + key.substr(key.find(' ') + 1, std::string::npos);
	if (type == "int") {
	  type_ = Type::INT;
	} else if (type == "bool") {
	  type_ = Type::BOOL;
	} else if (type == "varchar") {
	  type_ = Type::VARCHAR;
	} else if (type == "float") {
	  type_ = Type::FLOAT;
	} else if (type == "double") {
	  type_ = Type::DOUBLE;
	}
  }
  [[nodiscard]] std::string Name() const {
	return name_;
  }
  [[nodiscard]] Type Type() const {
	return type_;
  }

  [[nodiscard]] std::string Key() const {
	return key_;
  }
};

struct Row {
  std::unordered_map<std::string, std::string> data;
  Row(const std::vector<std::string> &values, const std::vector<Columns> &attributes) {
	for (size_t i = 0; i < attributes.size(); ++i) {
	  data[attributes[i].Name()] = values[i];
	}
  }
  Row(const std::vector<std::string> &values, const std::vector<std::string> &attributes) {
	for (size_t i = 0; i < attributes.size(); ++i) {
	  data[attributes[i]] = values[i];
	}
  }
  bool operator==(const Row& val) const {
	return data == val.data;
  }
  bool operator!=(const Row& val) const {
	return data != val.data;
  }
};

class Table {
 public:
  std::string name_;
  std::vector<Row> data_;
  std::vector<Columns> columns_;
 public:
  Table(const std::string &name, const std::vector<std::vector<std::string>> &columns) : name_(name) {
	for (const auto &x : columns) {
	  columns_.emplace_back(x[0], x[1], x[2]);
	}
  }

  Table() = default;

  void Insert(const std::vector<std::string> &values) {
	data_.emplace_back(values, columns_);
  }

};

class MyCoolDB {
 private:

  static std::string TableName(const std::string &input);

  static std::vector<std::vector<std::string>> Attributes(const std::string &input);

  static std::vector<std::vector<std::string>> Rows(const std::string &input);

  void LoadLine(const std::string &line);

  static void remove_special(std::string &val);

  static std::vector<std::string> tokenize(const std::string &query, char symbol);

  void Select(const std::smatch &match);

  void Update(const std::smatch &match);

  void Insert(const std::smatch &match);

  void Create(const std::smatch &match);

  void Delete(const std::smatch& match);

  void Join(const std::smatch& match);

 public:
  std::unordered_map<std::string, Table> tables_;

  void createTable(const std::string &name, const std::vector<std::vector<std::string>> &attributes) {
	tables_[name] = Table(name, attributes);
  }
  void dropTable(const std::string &name) {
	tables_.erase(name);
  }
  void insert(const std::string &name, const std::vector<std::string>& value) {
	tables_[name].Insert(value);
  }
  Table table(const std::string &name) {
	return tables_[name];
  }

  void SaveTofIle(const std::string &file);

  void LoadTables(const std::string &file);

  void Parse(const std::string &query);
};
