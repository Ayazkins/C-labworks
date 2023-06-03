#include "BD.h"

void MyCoolDB::LoadTables(const std::string &file) {
  std::ifstream input(file);
  std::string line;
  while (std::getline(input, line)) {
	this->Parse(line);
  }
}
void MyCoolDB::SaveTofIle(const std::string &file) {
  std::ofstream output(file);
  for (const auto &tablePair : tables_) {
	std::string create;
	create = "CREATE TABLE " + tablePair.first + " (";
	std::string buff = "(";
	for (const auto& x : tables_[tablePair.first].columns_) {
	  create += x.Name() + ' ';
	  buff += x.Name() + ", ";
	  if (x.Type() == Type::INT) {
		create += "int ";
	  } else if (x.Type() == Type::BOOL) {
		create += "bool ";
	  } else if (x.Type() == Type::VARCHAR) {
		create += "varchar ";
	  } else if (x.Type() == Type::FLOAT) {
		create += "float ";
	  } else if (x.Type() == Type::DOUBLE) {
		create += "double ";
	  }
	  create += x.Key() + ", ";
	}
	buff = buff.substr(0, buff.size() - 2);
	create = create.substr(0, create.size() - 2);
	buff += ")";
	create += ");\n";
	output << create;
	std::string insert;
	for (auto row : tables_[tablePair.first].data_) {
	  insert = "INSERT INTO " + tablePair.first + ' ' + buff + " VALUES (";
	  for (auto x : tables_[tablePair.first].columns_) {
		insert += row.data[x.Name()] + ", ";
	  }
	  insert = insert.substr(0, insert.size() - 2);
	  insert += ");\n";
	  output << insert;
	}
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
	std::vector<std::string> tokens = tokenize(token, ' ');
	values.push_back({tokens[0], tokens[1], tokens[2]});
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
	remove_special(text);
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
	  std::regex join
		  ("^" "SELECT (.+) FROM\\s+(\\w+)\\s+(INNER|LEFT|RIGHT)\\s+JOIN\\s+(\\w+)\\s+ON (.+)(?:\\s+(WHERE) (.+))?;");
	  std::regex for_create("^" "(SELECT) (.+) (FROM) (\\w+)(?: (WHERE) (.+))?;");
	  if (std::regex_search(usable_query, match, for_create)) {
		Select(match);
	  } else if (std::regex_search(usable_query, match, join)) {
		Join(match);
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
  std::regex where_regex(R"(\s*(\w+)\s*(=|IS)\s*(NOT\s*NULL|NULL|'?\w+\'?))");
  std::smatch where_match;
  std::vector<std::pair<std::string, std::string>> conditions;
  std::vector<std::pair<bool, bool>> and_or_vector;
  while (std::regex_search(where, where_match, where_regex)) {
	std::string attribute_name = where_match[1];
	std::string value = where_match[3];
	std::string operator_ = where_match[2];
	remove_special(operator_);
	remove_special(attribute_name);
	remove_special(value);
	if (operator_ == "IS") {
	  if (value == "NULL") {
		value = "NULL";
	  } else if (value == "NOT NULL") {
		value = "\\special\\";
	  }
	}
	std::regex and_or_regex("(AND|OR)");
	std::smatch and_or;
	bool and_ = false;
	bool or_ = false;
	if (std::regex_search(where, and_or, and_or_regex)) {
	  std::string operator_ = and_or[1];
	  remove_special(operator_);
	  if (operator_ == "OR") {
		or_ = true;
	  } else {
		and_ = true;
	  }
	} else {
	  and_ = true;
	}
	and_or_vector.emplace_back(or_, and_);
	conditions.emplace_back(attribute_name, value);
	where = where_match.suffix().str();
  }
  std::vector<Row> change;
  int index = 0;
  for (auto &x : table.data_) {
	std::vector<bool> conditions_result;
	for (const auto &cond : conditions) {
	  conditions_result.emplace_back(x.data[cond.first] == cond.second || (!x.data[cond.first].empty() && cond.second == "\\special\\") || (x.data[cond.first] == cond.second));
	}
	bool flag = conditions_result[0];
	int j = 1;
	for (int i = 0; i < conditions_result.size() - 1; ++i) {
	  if (and_or_vector[i].first) {
		flag = flag || conditions_result[j];
		j++;
	  } else if (and_or_vector[i].second) {
		flag = flag && conditions_result[j];
		j++;
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
	std::string not_null = "null";
	for (size_t i = 0; i < column.size(); ++i) {
	  if (i + 1 < x.size()) {
		if ((column[i] == "PRIMARY" || column[i] == "FOREIGN") && column[i + 1] == "KEY") {
		  key = column[i] + ' ' + column[i + 1];
		} else if (column[i] == "NOT" && column[i + 1] == "NULL") {
		  not_null = "not null";
		}
	  }
	}
	attributes.push_back({column[0], column[1], key, not_null});
  }
  tables_[table] = Table(table, attributes);
}
void MyCoolDB::Insert(const std::smatch &match) {
  Table &table = tables_[match[3]];
  std::vector<std::string> attributes = tokenize(match[4], ',');
  std::vector<std::string> values = tokenize(match[6], ',');
  std::vector<std::string> to_insert(table.columns_.size());
  for (const auto& x : table.columns_) {
	bool flag = false;
	for (const auto& y : attributes) {
	  if (x.Name() == y && x.NotNull()) {
		flag = true;
	  } else if (!x.NotNull()) {
		flag = true;
	  }
	}
	if (!flag) {
	  throw std::logic_error ("NOT NULL");
	}
  }
  for (size_t i = 0; i < attributes.size(); ++i) {
	remove_special(values[i]);
	remove_special(attributes[i]);
	for (const auto & x : table.columns_) {
	  if (x.Name() == attributes[i] && x.Key() == "PRIMARY KEY") {
		for (auto y : table.data_) {
		  if (y.data[attributes[i]] == values[i]) {
			throw std::logic_error ("PRIMARY KEY!!!!!");
		  }
		}
	  }
	}

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

  std::regex where_regex(R"(\s*(\w+)\s*(=|IS)\s*(NOT\s*NULL|NULL|'?\w+\'?))");
  std::smatch where_match;
  std::vector<std::pair<std::string, std::string>> conditions;
  std::vector<std::pair<bool, bool>> and_or_vector;
  while (std::regex_search(where, where_match, where_regex)) {
	std::string attribute_name = where_match[1];
	std::string value = where_match[3];
	std::string operator_ = where_match[2];
	remove_special(operator_);
	remove_special(attribute_name);
	remove_special(value);
	if (operator_ == "IS") {
	  if (value == "NULL") {
		value = "NULL";
	  } else if (value == "NOT NULL") {
		value = "\\special\\";
	  }
	}
	std::regex and_or_regex("(AND|OR)");
	std::smatch and_or;
	bool and_ = false;
	bool or_ = false;
	if (std::regex_search(where, and_or, and_or_regex)) {
	  std::string operator_ = and_or[1];
	  remove_special(operator_);
	  if (operator_ == "OR") {
		or_ = true;
	  } else {
		and_ = true;
	  }
	} else {
	  and_ = true;
	}
	and_or_vector.emplace_back(or_, and_);
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
  std::vector<Row*> change;
  int index = 0;
  for (auto &x : table.data_) {
	std::vector<bool> conditions_result;
	for (const auto &cond : conditions) {
	  conditions_result.emplace_back((x.data[cond.first].empty() && cond.second == "NULL") || (!x.data[cond.first].empty() && cond.second == "\\special\\") || (x.data[cond.first] == cond.second));
	}
	bool flag = conditions_result[0];
	int j = 1;
	for (int i = 0; i < conditions_result.size() - 1; ++i) {
	  if (and_or_vector[i].first) {
		flag = flag || conditions_result[j];
		j++;
	  } else if (and_or_vector[i].second) {
		flag = flag && conditions_result[j];
		j++;
	  }
	}
	if (flag) {
	  change.emplace_back(&x);
	}
  }

  for (auto x : change) {
	for (const auto &news : attributes_values) {
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
  std::regex where_regex(R"(\s*(\w+)\s*(=|IS)\s*(NOT\s*NULL|NULL|'?\w+\'?))");
  std::smatch where_match;
  std::vector<std::pair<std::string, std::string>> conditions;
  std::vector<std::pair<bool, bool>> and_or_vector;
  while (std::regex_search(where, where_match, where_regex)) {
	std::string attribute_name = where_match[1];
	std::string value = where_match[3];
	std::string operator_ = where_match[2];
	remove_special(operator_);
	remove_special(attribute_name);
	remove_special(value);
	if (operator_ == "IS") {
	  if (value == "NULL") {
		value = "NULL";
	  } else if (value == "NOT NULL") {
		value = "\\special\\";
	  }
	}
	std::regex and_or_regex("(AND|OR)");
	std::smatch and_or;
	bool and_ = false;
	bool or_ = false;
	if (std::regex_search(where, and_or, and_or_regex)) {
	  std::string operator_ = and_or[1];
	  remove_special(operator_);
	  if (operator_ == "OR") {
		or_ = true;
	  } else {
		and_ = true;
	  }
	} else {
	  and_ = true;
	}
	and_or_vector.emplace_back(or_, and_);
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
  int index = 0;
  for (auto &x : table.data_) {
	std::vector<bool> conditions_result;
	for (const auto &cond : conditions) {
	  conditions_result.emplace_back((x.data[cond.first].empty() && cond.second == "NULL") || (!x.data[cond.first].empty() && cond.second == "\\special\\") || (x.data[cond.first] == cond.second));
	}
	bool flag = conditions_result[0];
	int j = 1;
	for (int i = 0; i < conditions_result.size() - 1; ++i) {
	  if (and_or_vector[i].first) {
		flag = flag || conditions_result[j];
		j++;
	  } else if (and_or_vector[i].second) {
		flag = flag && conditions_result[j];
		j++;
	  }
	}
	if (flag) {
	  change.emplace_back(x);
	}
  }

  for (auto &row : change) {
	for (const auto &column : columns) {
	  if (!row.data[column].empty()) {
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
void MyCoolDB::Join(const std::smatch &match) {
  std::string attributes = match[1];
  std::string type = match[3];
  std::string on = match[5];
  std::string where = match[6];
  Table &table = tables_[match[2]];
  Table &add_table = tables_[match[4]];
  std::vector<std::string> attributes_vec;

  if (attributes == "*") {
	for (const auto &attribute : table.columns_) {
	  attributes_vec.emplace_back(table.name_ + '.' + attribute.Name());
	}
	for (const auto &attribute : add_table.columns_) {
	  attributes_vec.emplace_back(add_table.name_ + '.' + attribute.Name());
	}
  } else {
	attributes_vec = tokenize(attributes, ',');
  }

  std::vector<std::string> table_attributes;
  std::vector<std::string> join_attributes;
  for (auto &a : attributes_vec) {
	remove_special(a);
	std::string table_of_attribute_str = a.substr(0, a.find('.'));
	std::string name_of_attribute = a.substr(a.find('.') + 1);
	if (table_of_attribute_str == match[2]) {
	  table_attributes.emplace_back(name_of_attribute);
	} else if (table_of_attribute_str == match[4]) {
	  join_attributes.emplace_back(name_of_attribute);
	}
  }
  for (const auto &x : attributes_vec) {
	std::cout << x;
	if (x != *(attributes_vec.end() - 1)) {
	  std::cout << " | ";
	} else {
	  std::cout << '\n';
	}
  }
  std::regex for_conditon("\\s*([^\\s]+)\\s*=\\s*([^\\s\\,]+)\\s*");
  std::smatch condition;

  std::vector<std::pair<Row, Row>> rows;
  std::unordered_map<std::string, int> check;
  std::vector<Row> unused_left;
  std::vector<Row> unused_right;
  if (std::regex_search(on, condition, for_conditon)) {
	std::string left = condition[1];
	std::string right = condition[2];
	std::string left_attribute_name = left.substr(left.find('.') + 1);
	std::string right_attribute_name = right.substr(right.find('.') + 1);
	bool flag;
	for (auto x : table.data_) {
	  flag = false;
	  for (auto y : add_table.data_) {
		if (!x.data[left_attribute_name].empty() && x.data[left_attribute_name] == y.data[right_attribute_name]) {
		  rows.emplace_back(x, y);
		  //std::cout << "check" <<  x.data[left_attribute_name] << ' ' << y.data[right_attribute_name];
		  flag = true;
		}
	  }
	  if (!flag)
		unused_left.emplace_back(x);
	}

	for (auto x : add_table.data_) {
	  flag = false;
	  for (auto y : table.data_) {
		if (!x.data[right_attribute_name].empty() && x.data[right_attribute_name] == y.data[left_attribute_name]) {
		  flag = true;
		}
	  }
	  if (!flag)
		unused_right.emplace_back(x);
	}
  }

  for (auto y : rows) {
	for (const auto& x : table.columns_) {
	  std::cout << y.first.data[x.Name()] << " | ";
	}
	for (const auto& x: add_table.columns_) {
	  std::cout << y.second.data[x.Name()] << " | ";
	}
	std::cout << '\n';
  }

  if (type == "LEFT") {
	for (auto unused: unused_left) {
	  for (auto &x : table.columns_) {
		std::cout << unused.data[x.Name()] << " | ";
	  }
	  for (const auto& x : add_table.columns_) {
		std::cout << "NULL" << " | ";
	  }
	  std::cout << '\n';
	}
  }

  if (type == "RIGHT") {
	for (auto unused: unused_right) {
	  for (const auto& x : table.columns_) {
		std::cout << "NULL" << " | ";
	  }
	  for (const auto &x : add_table.columns_) {
		std::cout << unused.data[x.Name()] << " | ";
	  }
	  std::cout << '\n';
	}
  }
}