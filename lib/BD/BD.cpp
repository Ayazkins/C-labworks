#include "BD.h"

void MyCoolDB::LoadTables(const std::string &file) {
  std::ifstream input(file);
  std::string line;
  while (std::getline(input, line)) {
	LoadLine(line);
  }
}
void MyCoolDB::SaveTofIle(const std::string &file) {
  std::ofstream output(file);
  for (const auto &tablePair : tables_) {
	const std::string &tableName = tablePair.first;
	const Table &table = tablePair.second;
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
	  output << " ";
	  output << column.Key() << " ";
	  output << column.Name() << ' ';
	  if (i != table.columns_.size() - 1)
		output << ",";
	}
	output << ";";

	for (int i = 0; i < table.data_.size(); ++i) {
	  output << "Row:";
	  for (const auto &entry : table.columns_) {
		output << " " << table.data_[i].data.find(entry.Name())->second << ',';
	  }
	  output << ";";
	}
	output << "\n";
  }

  output.close();
}
void MyCoolDB::LoadLine(const std::string &line) {
  Table table(TableName(line), Attributes(line));
  for (const auto &x : Rows(line)) {
	table.Insert(x);
  }
  tables_[TableName(line)] = table;
}
std::vector<std::vector<std::string>> MyCoolDB::Rows(const std::string &input) {
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
  for (const std::string &row : rows) {
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
std::vector<std::vector<std::string>> MyCoolDB::Attributes(const std::string &input) {
  std::regex attributesRegex("Attributes: ([^;]+)");
  std::smatch match;
  std::regex_search(input, match, attributesRegex);
  std::string attributes = match[1];
  std::stringstream ss(attributes);
  std::vector<std::vector<std::string>> values;
  std::string token;
  while (std::getline(ss, token, ',')) {
	std::stringstream tokenStream(token);
	std::string type, attribute, key;
	if (tokenStream >> type >> key >> attribute) {
	  std::cout << type << ' ' << key << ' ' << attribute << '\n';
	  values.push_back({type, key, attribute});
	}
  }
  return values;
}
std::string MyCoolDB::TableName(const std::string &input) {
  std::regex tableRegex("tablename: ([^;]+)");
  std::smatch match;
  std::regex_search(input, match, tableRegex);
  std::string tablename = match[1];
  return tablename;
}

void MyCoolDB::Parse(const std::string &query) {
  std::regex command("^" "(CREATE TABLE|DROP TABLE|SELECT|(INSERT) (INTO)?|UPDATE|DELETE)" "(.*);");
  std::smatch match;
  std::string usable_query = query;
  remove_special(usable_query);
  while (std::regex_search(usable_query, match, command)) {
	std::string keyword = match[1];
	std::string text = match[2];
	remove_special(keyword);
	if (keyword == "DROP TABLE") {
	  auto it = tables_.find(text);
	  if (it != tables_.end()) {
		tables_.erase(it);
	  }
	} else if (keyword == "CREATE TABLE") {
	  std::regex for_create("^" "(CREATE TABLE) (\\w+)\\s*\\(([^;]+)\\);");
	  if (std::regex_search(usable_query, match, for_create)) {
		Create(match);
	  }
	} else if (keyword == "INSERT" || keyword == "INSERT INTO") {
	  std::regex for_create("^" "(INSERT) (INTO)? (\\w+)\\s*\\(([^;]+)\\) (VALUES) \\(([^;]+)\\);",
							std::regex_constants::icase);
	  if (std::regex_search(usable_query, match, for_create)) {
		Insert(match);
	  }
	} else if (keyword == "SELECT") {

	  std::regex for_create("^" "(SELECT) (.+) (FROM) (\\w+)(?: (WHERE) (.+))?;");
	  if (std::regex_search(usable_query, match, for_create)) {
		Select(match);
	  }
	} else if (keyword == "UPDATE") {
	  std::regex for_create("^" "(UPDATE) (\\w+) (SET) ([^;]+);");
	  if (std::regex_search(usable_query, match, for_create)) {
		Update(match);
	  }
	} else if (keyword == "DELETE") {
	  std::regex for_create("^" "(DELETE FROM) (\\w+)(?: (WHERE) ([^;]+))?;");
	  if (std::regex_search(usable_query, match, for_create)) {
		Delete(match);
	  }
	}
	usable_query = match.suffix().str();
	remove_special(usable_query);
  }
}
void MyCoolDB::Delete(const std::smatch &match) {
  Table &table = tables_[match[2]];
  std::string where = match[4];
  std::regex where_regex("\\s*([^\\s]+)\\s*([=])\\s*([^\\s\\,]+)\\s*");
  std::vector<std::pair<std::string, std::string>> conditions;
  std::smatch where_match;
  while (std::regex_search(where, where_match, where_regex)) {
	std::string attribute_name = where_match[1];
	std::string value = where_match[3];
	remove_special(attribute_name);
	remove_special(value);
	conditions.emplace_back(attribute_name, value);
	where = where_match.suffix().str();
  }
  std::vector<Row> change;
  for (auto &x : table.data_) {
	bool flag = true;
	for (const auto& cond : conditions) {
	  if (x.data[cond.first] != cond.second) {
		flag = false;
		break;
	  }
	}
	if (flag) {
	  change.emplace_back(x);
	}
  }
  std::vector<Row> buf = table.data_;
  table.data_.clear();
  for (const auto &x : buf) {
	bool flag = true;
	for (const auto &y : change) {
	  if (x.data == y.data) {
		flag = false;
		break;
	  }
	}
	if (flag) {
	  table.data_.emplace_back(x);
	}
  }
}
void MyCoolDB::Create(const std::smatch &match) {
  std::string table = match[2];
  std::vector<std::string> columns = tokenize(match[3], ',');
  std::vector<std::vector<std::string>> attributes;
  for (auto &x : columns) {
	remove_special(x);
	auto column = tokenize(x, ' ');
	std::string key = "NOT KEY";
	for (size_t i = 0; i < column.size(); ++i) {
	  if (i + 1 < x.size()) {
		if ((column[i] == "PRIMARY" || column[i] == "FOREIGN") && column[i + 1] == "KEY") {
		  key = column[i] + ' ' + column[i + 1];
		}
	  }
	}
	attributes.push_back({column[0], column[1], key});
  }
  tables_[table] = Table(table, attributes);
}
void MyCoolDB::Insert(const std::smatch &match) {
  Table &table = tables_[match[3]];
  std::vector<std::string> attributes = tokenize(match[4], ',');
  std::vector<std::string> values = tokenize(match[6], ',');
  std::vector<std::string> to_insert(table.columns_.size());
  for (size_t i = 0; i < attributes.size(); ++i) {
	remove_special(values[i]);
	remove_special(attributes[i]);
  }
  Row row(values, attributes);
  table.data_.emplace_back(row);
}
void MyCoolDB::Update(const std::smatch &match) {
  Table &table = tables_[match[2]];
  std::string text = match[4];
  std::string set;
  std::string where;
  if (text.find("WHERE") != std::string::npos) {
	size_t start = 0;
	size_t end = 0;
	while ((end = text.find("WHERE", start)) != std::string::npos) {
	  set = text.substr(start, end - start);
	  start = end + 5;
	}
	where = text.substr(start);
  } else {
	set = text;
  }
  remove_special(where);
  remove_special(set);
  std::regex set_regex("\\s*([^\\s]+)\\s*=\\s*([^\\s\\,]+)\\s*");
  std::smatch set_match;
  std::vector<std::pair<std::string, std::string>> attributes_values;
  while (std::regex_search(set, set_match, set_regex)) {
	std::string attribute = set_match[1];
	std::string value = set_match[2];
	remove_special(attribute);
	remove_special(value);
	attributes_values.emplace_back(attribute, value);
	set = set_match.suffix().str();
  }

  std::regex where_regex("\\s*([^\\s]+)\\s*([=])\\s*([^\\s\\,]+)\\s*");
  std::smatch where_match;
  std::vector<std::pair<std::string, std::string>> conditions;
  while (std::regex_search(where, where_match, where_regex)) {
	std::string attribute_name = where_match[1];
	std::string value = where_match[3];
	remove_special(attribute_name);
	remove_special(value);
	conditions.emplace_back(attribute_name, value);
	where = where_match.suffix().str();
  }
  if (conditions.empty()) {
	for (auto &x : table.data_) {
	  for (auto news : attributes_values) {
		x.data[news.first] = news.second;
	  }
	}
	return;
  }
  std::vector<Row *> change;
  for (auto &x : table.data_) {
	bool flag = true;
	for (const auto& cond : conditions) {
	  if (x.data[cond.first] != cond.second) {
		flag = false;
		break;
	  }
	}
	if (flag) {
	  change.emplace_back(&x);
	}
  }
  for (auto x : change) {
	for (const auto& news : attributes_values) {
	  (*x).data[news.first] = news.second;
	}
  }
}
void MyCoolDB::Select(const std::smatch &match) {
  Table &table = tables_[match[4]];
  std::string where = match[6];
  std::vector<std::string> columns;
  if (match[2] == "*") {
	for (const auto &column : table.columns_) {
	  columns.emplace_back(column.Name());
	}
  } else {
	for (auto &column : tokenize(match[2], ',')) {
	  remove_special(column);
	  columns.push_back(column);
	}
  }
  for (const auto &x : columns) {
	std::cout << x;
	if (x != *(columns.end() - 1)) {
	  std::cout << " | ";
	}
  }
  std::cout << '\n';
  std::regex where_regex("\\s*([^\\s]+)\\s*([=])\\s*([^\\s]+)\\s*");
  std::smatch where_match;
  std::vector<std::pair<std::string, std::string>> conditions;
  while (std::regex_search(where, where_match, where_regex)) {
	std::string attribute_name = where_match[1];
	std::string value = where_match[3];
	remove_special(attribute_name);
	remove_special(value);
	conditions.emplace_back(attribute_name, value);
	where = where_match.suffix().str();
  }
  if (conditions.empty()) {
	for (auto &row : table.data_) {
	  for (const auto &column : columns) {
		if (row.data.find(column) != row.data.end()) {
		  std::cout << row.data[column];
		} else {
		  std::cout << "NULL";
		}
		if (column != *(columns.end() - 1)) {
		  std::cout << " | ";
		}
	  }
	  std::cout << '\n';
	}
	return;
  }
  std::vector<Row> change;
  for (auto &x : table.data_) {
	bool flag = true;
	for (const auto& cond : conditions) {
	  if (x.data[cond.first] != cond.second) {
		flag = false;
		break;
	  }
	}
	if (flag) {
	  change.emplace_back(x);
	}
  }
  for (auto& row : change) {
	for (const auto &column : columns) {
	  if (row.data.find(column) != row.data.end()) {
		std::cout << row.data[column];
	  } else {
		std::cout << "NULL";
	  }
	  if (column != *(columns.end() - 1)) {
		std::cout << " | ";
	  }
	}
	std::cout << '\n';
  }
}
std::vector<std::string> MyCoolDB::tokenize(const std::string &query, const char symbol) {
  std::vector<std::string> tokens;
  std::stringstream ss(query);
  std::string token;

  while (std::getline(ss, token, symbol)) {
	if (!token.empty()) {
	  tokens.push_back(token);
	}
  }

  return tokens;
}
void MyCoolDB::remove_special(std::string &val) {
  const std::string not_allowed = " \t\f\v\n\r,";
  if (val.find_first_not_of(not_allowed) != std::string::npos
	  && val.find_last_not_of(not_allowed) != std::string::npos) {
	val = val.substr(val.find_first_not_of(not_allowed),
					 val.find_last_not_of(not_allowed) - val.find_first_not_of(not_allowed) + 1);
  } else {
	val.clear();
  }
}
