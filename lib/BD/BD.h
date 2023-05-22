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
 public:
  Columns(const std::string &name, const std::string &type) : name_(name) {
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
  Type Type() const {
	return type_;
  }
};

struct Row {
  std::unordered_map<std::string, std::string> data;
 public:
  Row(const std::vector<std::string> &values, const std::vector<Columns> &attributes) {
	for (size_t i = 0; i < attributes.size(); ++i) {
	  data[attributes[i].Name()] = values[i];
	}
  }
};

class Table {
 public:
  std::string name_;
  std::vector<Row> data_;
  std::vector<Columns> columns_;
 public:
  Table(const std::string &name, const std::vector<std::pair<std::string, std::string>> &columns) : name_(name) {
	for (const auto &x : columns) {
	  columns_.emplace_back(x.first, x.second);
	}
  }

  Table() = default;

  void Insert(const std::vector<std::string> &values) {
	data_.emplace_back(values, columns_);
  }

};

class MyCoolDB {
 private:
  std::unordered_map<std::string, Table> tables_;

  std::string TableName(const std::string& input) {
	std::regex tableRegex("tablename: ([^;]+)");
	std::smatch match;
	std::regex_search(input, match, tableRegex);
	std::string tablename = match[1];
	return tablename;
  }

  std::vector<std::pair<std::string, std::string>> Attributes(const std::string& input) {
	std::regex attributesRegex("Attributes: ([^;]+)");
	std::smatch match;
	std::regex_search(input, match, attributesRegex);
	std::string attributes = match[1];
	std::stringstream ss(attributes);
	std::vector<std::pair<std::string, std::string>> values;
	std::string token;
	while (std::getline(ss, token, ',')) {
	  std::stringstream tokenStream(token);
	  std::string type, attribute;
	  if (tokenStream >> type >> attribute) {
		values.emplace_back(attribute, type);
	  }
	}
	return values;
  }

  std::vector<std::vector<std::string>> Rows(const std::string& input) {
	std::regex rowRegex("Row: ([^;]+)");
	std::regex valueRegex("(\\b[^,;]+)");
	std::vector<std::string> rows;
	std::smatch rowMatch;
	std::string::const_iterator inputIt(input.cbegin()), inputEnd(input.cend());
	while (std::regex_search(inputIt, inputEnd, rowMatch, rowRegex)) {
	  std::string rowValues = rowMatch[1];
	  rows.push_back(rowValues);
	  inputIt = rowMatch[0].second;
	}
	std::vector<std::vector<std::string>> out;
	for (const std::string& row : rows) {
	  out.emplace_back();
	  std::regex_iterator<std::string::const_iterator> valueIterator(row.cbegin(), row.cend(), valueRegex);
	  std::regex_iterator<std::string::const_iterator> valueEnd;
	  while (valueIterator != valueEnd) {
		std::string value = (*valueIterator)[1];
		out.back().emplace_back(value);
		++valueIterator;
	  }
	}
	return out;
  }

  void LoadLine(const std::string& line) {
	Table table(TableName(line), Attributes(line));
	for (const auto& x: Rows(line)) {
	  table.Insert(x);
	}
	tables_[TableName(line)] = table;
  }

 public:

  void createTable(const std::string &name, const std::vector<std::pair<std::string, std::string>> &attributes) {
	  tables_[name] = Table(name, attributes);
  }
  void dropTable(const std::string& name) {
	tables_.erase(name);
  }
  void insert(const std::string& name, const std::vector<std::string> value) {
	tables_[name].Insert(value);
  }
  Table table(const std::string& name) {
	return tables_[name];
  }

  void Upload(const std::string& file) {
	std::ofstream output(file);
	for (const auto& tablePair : tables_) {
	  const std::string& tableName = tablePair.first;
	  const Table& table = tablePair.second;
	  output << "tablename: " << tableName << ";";
	  output << "Attributes:";
	  for (int i = 0; i < table.columns_.size(); ++i) {
		output << ' ';
		auto column = table.columns_[i];
		if (column.Type() == Type::INT) {
		  output << "int";
		} else if (column.Type() == Type::BOOL) {
		  output << "bool";
		} else if (column.Type() == Type::VARCHAR) {
		  output << "varchar";
		} else if (column.Type() == Type::FLOAT) {
		  output << "float";
		} else if (column.Type() == Type::DOUBLE) {
		  output << "double";
		}
		output << " " << column.Name() << ' ';
		if (i != table.columns_.size() - 1)
			output << ",";
	  }
	  output << ";";
	  for (const Row& row : table.data_) {
		output << "Row:";
		for (const auto& entry : row.data) {
		  output << " " << entry.second << ',';
		}
		output << ";";
	  }
	}
	output.close();
  }

  void LoadTables(const std::string& file) {
	std::ifstream input(file);
	std::string line;
	while (std::getline(input, line)) {
	  LoadLine(line);
	}
  }

};
