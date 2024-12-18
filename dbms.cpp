#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <unordered_map> 
#include <algorithm> 
#include <limits> 
#include <mutex>
#include <thread>  
#include <tuple>
#include <functional>
#include <map> 
#include <numeric> 
#include <regex>  

// Custom hash function for tuples
struct TupleHasher {
    template <typename T>
    std::size_t operator()(const T& tuple) const {
        auto hash1 = std::hash<std::tuple_element_t<0, T>>()(std::get<0>(tuple));
        auto hash2 = std::hash<std::tuple_element_t<1, T>>()(std::get<1>(tuple));
        return hash1 ^ (hash2 << 1);  // Combine the hashes
    }
};

struct Condition {
    std::string column;
    std::string op;
    std::string value;
    std::vector<Condition> subconditions;
    std::string logicalOp;

    bool isGroup;

    Condition() : isGroup(false){}
}; 

struct OrderBy {
    std::string column; // Column to sort on
    bool ascending;     // true = ASC, false = DESC
};

struct ConditionGroup {
    std::vector<Condition> conditions;
    std::string logicalOp;  // "AND" or "OR" applied within this group
    std::vector<ConditionGroup> subgroups;
};

struct Index {
    std::string column;
    std::unordered_map<std::string, std::vector<size_t>> indexMap;
};

enum class DataType {
    INTEGER,
    STRING,
    DATE
};

class Transaction {
public:
    std::vector<std::vector<std::string>> inserts;
    std::vector<std::pair<size_t, std::vector<std::string>>> updates;  // (row index, new data)
    std::vector<size_t> deletes;  // Row indices

    void clear() {
        inserts.clear();
        updates.clear();
        deletes.clear();
    }
};

class Table {
private:
    std::string tableName;
    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> columns;
    std::vector<DataType> columnTypes; // store data type of each column

    std::unordered_map<std::string, size_t> index;
    // Multi-column index for faster querying based on multiple columns
    std::unordered_map<std::tuple<std::string, std::string>, size_t, TupleHasher> multiColumnIndex;
    bool inTransaction = false;
    std::vector<std::vector<std::string>> transactionBackup;

    // Lock for concurrency control
    std::mutex tableMutex;
    std::vector<Index> indexes;
    std::vector<Transaction> transactions; 

public:

    // Default Constructor
    // Table() = default;

    Table(const std::string& name, const std::vector<std::string>& colNames, const std::vector<DataType>& colTypes): tableName(name), columns(colNames), columnTypes(colTypes) {
        std::cout << "Table '" << tableName << "' created with columns: ";
        for (const auto& col : columns) {
            std::cout << col << " ";
        }
        std::cout << std::endl;
    }


    // Table joinTables(const Table& other, const std::string& joinColumn) const {
    //     // Find the column index in both tables
    //     auto it1 = std::find(columns.begin(), columns.end(), joinColumn);
    //     auto it2 = std::find(other.columns.begin(), other.columns.end(), joinColumn);

    //     if(it1 == columns.end() || it2 == other.columns.end()){
    //         throw std::runtime_error("Join column not found in one or both tables.");
    //     }

    //     size_t index1 = std::distance(columns.begin(), it1);
    //     size_t index2 = std::distance(other.columns.begin(), it2);

    //     // create result Table
    //     Table result;
    //     result.columns = columns;
    //     result.columns.insert(result.columns.end(),
    //                         other.columns.begin(),
    //                         other.columns.end());

    //     // Perform the join 
    //     for(const auto& row1 : rows){
    //         for(const auto& row2 : other.rows){
    //             if(row1[index1] == row1[index2]){
    //                 std::vector<std::string> combinedRow = row1;
    //                 combinedRow.insert(combinedRow.end(), row2.begin(), row2.end());
    //                 result.rows.push_back(combinedRow);
    //             }
    //         }
    //     }

    //     return result;
    // }

    // Add a row of data
    void addRow(std::vector<std::string> rowData) {
        std::lock_guard<std::mutex> lock(tableMutex);  // Lock for concurrency

        if (rowData.size() != columns.size()) {
            std::cout << "Error: Row size (" << rowData.size() << ") does not match the number of columns (" << columns.size() << ")." << std::endl;
            return;
        }

        for(size_t i = 0; i < rowData.size(); ++i){
            if(!isValidDataType(rowData[i], columnTypes[i])){
                std::cout << "Error: Invalid data type for column '" << columns[i] << "'." << std::endl;
                return;
            }
        }

        rows.push_back(rowData);  // Add row to the table

        // Try to find the index of the "ID" column for indexing (if present)
        size_t idIndex = -1;
        for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i] == "ID") {
                idIndex = i;
                break;
            }
        }

        // If the table has an "ID" column, update the index
        if (idIndex != -1) {
            // For tables like "Grades" that don't have an "Age" column, we just index by "ID"
            multiColumnIndex[std::make_tuple(rowData[idIndex], "")] = rows.size() - 1;  // Added empty string for the second element

            std::cout << "Row added and indexed by 'ID'." << std::endl;
        } else {
            std::cout << "Row added without 'ID' indexing." << std::endl;
        }
    }


    void displayTable() {
        std::cout << "Table: " << tableName << std::endl;
        for(const auto& col : columns){
            std::cout<< col << "\t";
        }

        std::cout << std::endl;
        
        for(const auto& row : rows){
            for(const auto& data : row){
                std::cout << data << "\t";
            }
            std::cout << std::endl;
        }
    }

    int colunmFind(const std::string& columnName){
        size_t columnIndex = -1;
        for(size_t i=0; i < columns.size(); i++){
            if(columns[i] == columnName){
                columnIndex = i;
                break;
            }
        }

        if (columnIndex == -1) {
            std::cout << "Error: Column '" << columnName << "' not found." << std::endl;
            return 0;
        }

        return static_cast<int>(columnIndex); ;
    }

    std::vector<std::vector<std::string>> selectRows(const std::string& columnName, const std::string& value) {
        std::vector<std::vector<std::string>> result;

        if(columnName == "ID"){
            auto it = index.find(value);
            if(it != index.end()){
                result.push_back(rows[it->second]);
            }
        }else {
            // Find the index of the column with the given name
            size_t columnIndex = -1;
            for (size_t i = 0; i < columns.size(); i++) {
                if (columns[i] == columnName) {
                    columnIndex = i;
                    break;
                }
            }

            // If the column is not found, return an empty result
            if (columnIndex == -1) {
                std::cout << "Error: Column '" << columnName << "' not found." << std::endl;
                return result;
            }

            // Iterate over the rows and check if the value in the specified column matches
            for (const auto& row : rows) {
                if (row[columnIndex] == value) {
                    result.push_back(row);
                }
            }
        }

        return result;
    }

    void updateRows(const std::string& columnName, const std::string& matchValue, const std::string& updateColumn, const std::string& newValue){
        std::lock_guard<std::mutex> lock(tableMutex); 
        size_t matchColumnIndex = -1;
        size_t updateColumnIndex = -1;
        // checking and getting the index of columns
        for(size_t i = 0; i < columns.size(); i++){
            if(columns[i] == columnName){
                matchColumnIndex = i;
            }
            if(columns[i] == updateColumn){
                updateColumnIndex = i;
            }
        }
        // Check if both columns exist
        if (matchColumnIndex == -1 || updateColumnIndex == -1) {
            std::cout << "Error: Column not found." << std::endl;
            return;
        }
        // update columns
        for(auto& row: rows){
            if(row[matchColumnIndex] == matchValue){
                row[updateColumnIndex] = newValue;
            }
        }
        std::cout << "Rows updated where " << columnName << " == " << matchValue << std::endl;
    }

    void saveToFile(const std::string& filename){
        std::ofstream outFile(filename);

        if(outFile.is_open()){
            // write the table name
            outFile << tableName << std::endl;

            // write col name
            for(const auto& col : columns){
                outFile << col << "\t";
            }

            outFile << std::endl;

            //write rows
            for(const auto& row : rows){
                for(const auto& data: row){
                    outFile << data << "\t";
                }
                outFile << std::endl;
            }

            outFile.close();
            std::cout << "Data saved to " << filename << std::endl;

        }else {
            std::cout << "Error: Unable to open file for writing." << std::endl;
        }
    }

    void deleteRows(const std::string& columnName, const std::string& value){
        std::lock_guard<std::mutex> lock(tableMutex);  

        int columnIndex = colunmFind(columnName);

        // itreate over the rows and delete matching one
        for(auto it = rows.begin(); it != rows.end();){
            if ((*it)[columnIndex] == value) {
                it = rows.erase(it);  // Remove the row and get a new iterator
            } else {
                ++it;  // Move to the next row
            }

            std::cout << "Rows deleted where " << columnName << " == " << value << std::endl;
        }
    }

    void loadFromFile(const std::string& fileName){
        std::ifstream inFile(fileName);

        if(inFile.is_open()){
            std::string line;

            // clear existing row (incase we are loading into existing table)
            rows.clear();

            // read table name
            std::getline(inFile, tableName);

            // read and skip the columns
            std::getline(inFile, line);

            // read each row of data
            while(std::getline(inFile, line)){
                std::stringstream ss(line);
                std::vector<std::string> rowData;
                std::string data;
                while(ss >> data){
                    rowData.push_back(data);
                }
                rows.push_back(rowData);
            }

            inFile.close();

            std::cout << "Data loaded from " << fileName << std::endl;
        } else {
            std::cout << "Error: Unable to open file for reading." << std::endl;
        }
    }

    void sortRows(const std::string& columnName, bool ascending = true){
       int columnIndex = colunmFind(columnName);

        // Sort the rows based on the selected column
        std::sort(rows.begin(), rows.end(), [columnIndex, ascending](const std::vector<std::string>& row1, const std::vector<std::string>& row2) {
            if (ascending) {
                return row1[columnIndex] < row2[columnIndex];  // Ascending order
            } else {
                return row1[columnIndex] > row2[columnIndex];  // Descending order
            }
        });

        std::cout << "Rows sorted by column '" << columnName << "' in " << (ascending ? "ascending" : "descending") << " order." << std::endl;
    }

    size_t countRows() const {
        return rows.size();
    }

    // double sumColumn(const std::string& columnName){
    //     int columnIndex = colunmFind(columnName);
    //     double sum = 0;
    //     for(const auto& row : rows){
    //         try{
    //             sum += std::stod(row[columnIndex]);
    //         } catch (const std::invalid_argument) {
    //             std::cout << "Skipping non-number value:" << row[columnIndex] << std::endl;
    //         } 
    //     }
    //     return sum;
    // }

    // double minColumn(const std::string& columnName){
    //     int columnIndex = colunmFind(columnName);
    //     double minValue = std::numeric_limits<double>::max();
    //     for(const auto& row : rows){
    //         try{
    //             double value = std::stod(row[columnIndex]);
    //             if(value < minValue){
    //                 minValue = value;
    //             }
    //         }catch (const std::invalid_argument) {
    //             std::cout<< "Skipping non-numberic value: " << row[columnIndex] << std::endl;
    //         }
    //     }
    //     return minValue;
    // }

    // double maxColumn(const std::string& columnName) {
    //     int columnIndex = colunmFind(columnName);
    //     double maxValue = std::numeric_limits<double>::lowest();
    //     for (const auto& row : rows) {
    //         try {
    //             double value = std::stod(row[columnIndex]);
    //             if (value > maxValue) {
    //                 maxValue = value;
    //             }
    //         } catch (const std::invalid_argument&) {
    //             std::cout << "Skipping non-numeric value: " << row[columnIndex] << std::endl;
    //         }
    //     }
    //     return maxValue;
    // }

    double averageColumn(const std::string& columnName) {
        double sum = sumColumn(columnName);
        size_t count = countRows();
        return (count > 0) ? sum / count : 0;
    }

    void commit(){
        if(!inTransaction){
            std::cout << "No active Transaction to commit." << std::endl;
            return;
        }
        inTransaction = false;
        transactionBackup.clear();
        std::cout << "Transaction committed." << std::endl; 
    }

    void rollback(){
        if(!inTransaction){
            std::cout << "No active transaction to rollback." << std::endl;
            return;
        }
        rows = transactionBackup;
        transactionBackup.clear();
        std::cout<< "Transaction rolled back." << std::endl;
    };

    std::vector<std::string> queryByIdAndAge(const std::string& id, const std::string& age){
        std::lock_guard<std::mutex> lock(tableMutex); 

        auto it = multiColumnIndex.find(std::make_tuple(id, age));
        if(it != multiColumnIndex.end()){
            return rows[it->second];
        }else {
             std::cout << "No row found with ID == " << id << " and Age == " << age << std::endl;
            return {};
        }
    }

    std::vector<std::vector<std::string>> join(const Table& otherTable, const std::string& columnName){
        std::lock_guard<std::mutex> lock(tableMutex); 

        size_t thisColunIndex = -1;
        size_t otherColunIndex = -1;

        for(size_t i=0; i < columns.size(); i++){
            if(columns[i] == columnName){
                thisColunIndex = i;
                break;
            }
        }

        for(size_t i = 0; i < otherTable.columns.size(); i++){
            if(otherTable.columns[i] == columnName){
                otherColunIndex = i;
                break;
            }
        }

        if(thisColunIndex == -1 || otherColunIndex == -1){
            std::cout << "Error: Column '" << columnName << "' not found in one or both tables." << std::endl;
            return {};
        }


        // Vector to store the result of the join
        std::vector<std::vector<std::string>> result;

        // perfom the join: itreate through this table's rows
        for(const auto& thisRow : rows){
            // Itreate through the other table's rows
            for(const auto& otherRow : otherTable.rows){
                // If the join column matches in both tables, combine the row
                if(thisRow[thisColunIndex] == otherRow[otherColunIndex]){
                    std::vector<std::string> joinedRow = thisRow;
                    // add colums from current table to this
                    joinedRow.insert(joinedRow.end(), otherRow.begin(), otherRow.end());

                    // add columms from the other table, skipping the join column to avoid duplication
                    for(size_t i=0; i < otherRow.size(); i++){
                        if(i != otherColunIndex){
                            joinedRow.push_back(otherRow[i]);
                        }
                    }

                    // add the combined row to the result
                    result.push_back(joinedRow);
                }
            }
        }

        return result;
    }

    // single col
    // void sortTable(const std::string& columnName, bool ascending = true){
    //     std::lock_guard<std::mutex> lock(tableMutex);
    //     //Find the index of the column to sort by
    //     size_t columnIndex  = 1;
    //     for(size_t i =0; i < columns.size(); i++){
    //         if(columns[i] == columnName){
    //             columnIndex = i;
    //             break;
    //         }
    //     }
    //     if(columnIndex == -1){
    //         std::cout << "Error: Column '" << columnName <<"' not found." << std::endl;
    //         return;
    //     }
    //     // Sort the rows based on the selected column
    //     std::sort(rows.begin(), rows.end(), [columnIndex, ascending](const std::vector<std::string>& row1, const std::vector<std::string>& row2) {
    //         if (ascending) {
    //             return row1[columnIndex] < row2[columnIndex];  // Ascending order
    //         } else {
    //             return row1[columnIndex] > row2[columnIndex];  // Descending order
    //         }
    //     });
    //     std::cout << "Table sorted by column '" << columnName << "' in " << (ascending ? "ascending" : "descending") << " order." << std::endl;
    // }

    void sortTable(const std::vector<std::string>& columnNames, const std::vector<bool>& ascendingFlags) {
        std::lock_guard<std::mutex> lock(tableMutex);  // Lock for concurrency

        // Find the indices of the columns to sort by
        std::vector<size_t> columnIndices;
        for (const auto& columnName : columnNames) {
            size_t columnIndex = -1;
            for (size_t i = 0; i < columns.size(); i++) {
                if (columns[i] == columnName) {
                    columnIndex = i;
                    break;
                }
            }

            if (columnIndex == -1) {
                std::cout << "Error: Column '" << columnName << "' not found." << std::endl;
                return;
            }

            columnIndices.push_back(columnIndex);
        }

        // Sort the rows based on multiple columns
        std::sort(rows.begin(), rows.end(), [columnIndices, ascendingFlags](const std::vector<std::string>& row1, const std::vector<std::string>& row2) {
            for (size_t i = 0; i < columnIndices.size(); ++i) {
                size_t columnIndex = columnIndices[i];
                bool ascending = ascendingFlags[i];

                if (row1[columnIndex] != row2[columnIndex]) {
                    if (ascending) {
                        return row1[columnIndex] < row2[columnIndex];
                    } else {
                        return row1[columnIndex] > row2[columnIndex];
                    }
                }
            }
            return false;  // If all specified columns are equal, maintain the original order
        });

        std::cout << "Table sorted by columns: ";
        for (const auto& columnName : columnNames) {
            std::cout << columnName << " ";
        }
        std::cout << std::endl;
    }

    double sumColumn(const std::string& columnName){
        std::lock_guard<std::mutex> lock(tableMutex);

        size_t columnIndex = -1;
        for (size_t i = 0; i < columns.size(); i++) {
            if (columns[i] == columnName) {
                columnIndex = i;
                break;
            }
        }

        if (columnIndex == -1) {
            std::cout << "Error: Column '" << columnName << "' not found." << std::endl;
            return 0;
        }

        double sum = 0.0;

        for(const auto& row : rows){
            try{
                sum += std::stod(row[columnIndex]); // convert string to double and add to sum
            } catch (const std::invalid_argument&) {
                std::cout << "Skipping non-numeric value: " << row[columnIndex] << std::endl;
            }
        }
        return sum;
    }

    double avgColumn(const std::string& columnName) {
        std::lock_guard<std::mutex> lock(tableMutex);  // Lock for concurrency

        size_t columnIndex = -1;
        for (size_t i = 0; i < columns.size(); i++) {
            if (columns[i] == columnName) {
                columnIndex = i;
                break;
            }
        }

        if (columnIndex == -1) {
            std::cout << "Error: Column '" << columnName << "' not found." << std::endl;
            return 0;
        }

        double sum = 0.0;
        int count = 0;
        for (const auto& row : rows) {
            try {
                sum += std::stod(row[columnIndex]);
                count++;
            } catch (const std::invalid_argument&) {
                std::cout << "Skipping non-numeric value: " << row[columnIndex] << std::endl;
            }
        }
        return count == 0 ? 0 : sum / count;
    }

    double minColumn(const std::string& columnName) {
        std::lock_guard<std::mutex> lock(tableMutex);  // Lock for concurrency

        size_t columnIndex = -1;
        for (size_t i = 0; i < columns.size(); i++) {
            if (columns[i] == columnName) {
                columnIndex = i;
                break;
            }
        }

        if (columnIndex == -1) {
            std::cout << "Error: Column '" << columnName << "' not found." << std::endl;
            return 0;
        }

        double minValue = std::numeric_limits<double>::max();
        bool foundNumeric = false;
        for (const auto& row : rows) {
            try {
                double value = std::stod(row[columnIndex]);
                minValue = std::min(minValue, value);
                foundNumeric = true;
            } catch (const std::invalid_argument&) {
                std::cout << "Skipping non-numeric value: " << row[columnIndex] << std::endl;
            }
        }
        return foundNumeric ? minValue : 0;  // Return 0 if no numeric values were found
    }

    double maxColumn(const std::string& columnName) {
        std::lock_guard<std::mutex> lock(tableMutex);  // Lock for concurrency

        size_t columnIndex = -1;
        for (size_t i = 0; i < columns.size(); i++) {
            if (columns[i] == columnName) {
                columnIndex = i;
                break;
            }
        }

        if (columnIndex == -1) {
            std::cout << "Error: Column '" << columnName << "' not found." << std::endl;
            return 0;
        }

        double maxValue = std::numeric_limits<double>::lowest();
        bool foundNumeric = false;
        for (const auto& row : rows) {
            try {
                double value = std::stod(row[columnIndex]);
                maxValue = std::max(maxValue, value);
                foundNumeric = true;
            } catch (const std::invalid_argument&) {
                std::cout << "Skipping non-numeric value: " << row[columnIndex] << std::endl;
            }
        }
        return foundNumeric ? maxValue : 0;  // Return 0 if no numeric values were found
    }

    std::vector<std::vector<std::string>> filterRows(const std::string& columnName, const std::string& op, const std::string& value){
        std::lock_guard<std::mutex> lock(tableMutex);

        size_t columnIndex = -1;
        for(size_t i = 0; i< columns.size(); i++){
            if(columns[i] == columnName){
                columnIndex = i;
                break;
            }
        }

        if(columnIndex == -1){
            std::cout << "Error: Column '" << columnName << "' not found" << std::endl;
            return {};
        }

        // Vector to store filtered rows
        std::vector<std::vector<std::string>> result;

        for(const auto& row : rows){
            bool conditionMet = false;
            try {
                // convert the row data and filters value to double if it's a numeric comparison
                double rowValue = std::stod(row[columnIndex]);
                double filterValue = std::stod(value);

                if(op == "==") conditionMet = (rowValue == filterValue);
                else if (op == "!=") conditionMet = (rowValue != filterValue);
                else if (op == "<") conditionMet = (rowValue < filterValue);
                else if (op == ">") conditionMet = (rowValue > filterValue);
                else if (op == "<=") conditionMet = (rowValue <= filterValue);
                else if (op == ">=") conditionMet = (rowValue >= filterValue);
            } catch (const std::invalid_argument&) {
                // Handle string comparisons
                if (op == "==") conditionMet = (row[columnIndex] == value);
                else if (op == "!=") conditionMet = (row[columnIndex] != value);
            }

             // If the condition is met, add the row to the result
            if (conditionMet) {
                result.push_back(row);
            }
        }
        return result;
    }

    std::map<std::string, double> groupedAggregation(const std::string& groupByColumn, const std::string& aggColumn, const std::string& aggType){
    
        // Find the indices of the columns
        size_t groupByIndex = -1, aggColumnIndex = -1;
        for (size_t i = 0; i < columns.size(); i++) {
            if (columns[i] == groupByColumn) groupByIndex = i;
            if (columns[i] == aggColumn) aggColumnIndex = i;
        }

        if (groupByIndex == -1 || aggColumnIndex == -1) {
            std::cout << "Error: Invalid column name(s)." << std::endl;
            return {};
        }

        // Map to store the groups and aggregation values
        std::map<std::string, std::vector<double>> groups;

        // Group the rows based on the grouping column
        for (const auto& row : rows){
            const std::string& groupValue = row[groupByIndex];
            // attempt to convert the aggregation column value to double
            try{
                double value = std::stod(row[aggColumnIndex]);
                groups[groupValue].push_back(value); // add value to the group
            } catch (const std::invalid_argument&){
                std::cout << "Skipping non-numeric value in aggregation column: " << row[aggColumnIndex] << std::endl;
            }
        }

        // Map to store the final aggregation result for each group
        std::map<std::string, double> result;

        // calculate the specified aggregation for each group
        for(const auto& group : groups){
            const std::string& groupKey = group.first;
            const std::vector<double>& values = group.second;
            double aggregationValue = 0.0;

             if (aggType == "SUM") {
                aggregationValue = std::accumulate(values.begin(), values.end(), 0.0);
            } else if (aggType == "AVG") {
                aggregationValue = values.empty() ? 0 : std::accumulate(values.begin(), values.end(), 0.0) / values.size();
            } else if (aggType == "MIN") {
                aggregationValue = *std::min_element(values.begin(), values.end());
            } else if (aggType == "MAX") {
                aggregationValue = *std::max_element(values.begin(), values.end());
            } else {
                std::cout << "Error: Invalid aggregation type." << std::endl;
                return {};
            }

            result[groupKey] = aggregationValue;
        }

        return result;
    }

    void conditionUpdateRow(const std::string& targetColumn, const std::string& newValue, const std::string& conditionColumn, const std::string& op, const std::string& conditionValue){
        // Find indices of the target and condition columns
        size_t targetIndex = -1, conditionIndex = -1;
        for (size_t i = 0; i < columns.size(); i++) {
            if (columns[i] == targetColumn) targetIndex = i;
            if (columns[i] == conditionColumn) conditionIndex = i;
        }

        if (targetIndex == -1 || conditionIndex == -1) {
            std::cout << "Error: Column not found." << std::endl;
            return;
        }

        // update rows that match the condition
        for(auto& row : rows){
            bool conditionMet = false;
            try
            {
                // Try to convert the condition value and the row value to double for numeric comparison
                double rowValue = std::stod(row[conditionIndex]);
                double condValue = std::stod(conditionValue);

                if (op == "==") conditionMet = (rowValue == condValue);
                else if (op == "!=") conditionMet = (rowValue != condValue);
                else if (op == "<") conditionMet = (rowValue < condValue);
                else if (op == ">") conditionMet = (rowValue > condValue);
                else if (op == "<=") conditionMet = (rowValue <= condValue);
                else if (op == ">=") conditionMet = (rowValue >= condValue);

            }
            catch(const std::invalid_argument&)
            {
                // Handle string comparison if values are non-numeric
                if (op == "==") conditionMet = (row[conditionIndex] == conditionValue);
                else if (op == "!=") conditionMet = (row[conditionIndex] != conditionValue);
            }

            if(conditionMet){
                row[targetIndex] = newValue;
                std::cout<< "Updated row: ";
                for(const auto& value : row){
                    std::cout<<value<<"\t";
                }  
                std::cout << std::endl;
            }
        }
    }

    void conditionDeleteRow(const std::string& conditionColumn, const std::string& op, const std::string& conditionValue){
        // Find the index of the condition column
        size_t conditionIndex = -1;
        for (size_t i = 0; i < columns.size(); i++) {
            if (columns[i] == conditionColumn) {
                conditionIndex = i;
                break;
            }
        }

        if (conditionIndex == -1) {
            std::cout << "Error: Column '" << conditionColumn << "' not found." << std::endl;
            return;
        }

        for(auto it = rows.begin(); it!=rows.end();){
            bool conditionMet = false;
            try
            {
                // Try to convert the condition value and the row value to double for numeric comparison
                double rowValue = std::stod((*it)[conditionIndex]);
                double condValue = std::stod(conditionValue);

                if (op == "==") conditionMet = (rowValue == condValue);
                else if (op == "!=") conditionMet = (rowValue != condValue);
                else if (op == "<") conditionMet = (rowValue < condValue);
                else if (op == ">") conditionMet = (rowValue > condValue);
                else if (op == "<=") conditionMet = (rowValue <= condValue);
                else if (op == ">=") conditionMet = (rowValue >= condValue);
            } catch (const std::invalid_argument&) {
                // Handle string comparison if values are non-numeric
                if (op == "==") conditionMet = ((*it)[conditionIndex] == conditionValue);
                else if (op == "!=") conditionMet = ((*it)[conditionIndex] != conditionValue);
            }

            // If the condition is met, delete the row and move the iterator to the next element
            if(conditionMet) {
                std::cout << "Deleting row: ";
                for(const auto& value : *it){
                    std::cout << value << "\t";
                }
                std::cout << std::endl;
                it = rows.erase(it);
            }else {
              ++it;  
            }
            
        }
    }

    void exportToCSV(const std::string& fileName){
        std::lock_guard<std::mutex> lock(tableMutex);  // Lock for concurrency

        // open the file for writing 
        std::ofstream outFile(fileName);
        if(!outFile.is_open()){
            std::cout << "Error: Could not open file " << fileName << " for writing." << std::endl;
            return;
        }

        for(size_t i = 0; i < columns.size(); i++){
            outFile << columns[i];
            if(i < columns.size() -1) outFile << "," ; // Add comma between columns
        }
        outFile << "\n"; // Newline after headers

        // write each row
        for(const auto& row : rows){
            for(size_t i = 0; i < row.size(); ++i){
                outFile << row[i];
                if(i < row.size() - 1) outFile << ","; // Add comma between columns
            }
            outFile << "\n";
        }

        outFile.close();
        std::cout << "Table data successfully exported to " << fileName << std::endl;
    }
    
    void importFromCSV(const std::string& filename, bool clearExisingData = true){
        std::lock_guard<std::mutex> lock(tableMutex);  // Lock for concurrency

        // open file for reading
        std::ifstream inFile(filename);
        if(!inFile.is_open()){
            std::cout << "Error: Could not open file " << filename << " for reading." << std::endl;
            return;
        }

        if(clearExisingData){
            columns.clear();
            rows.clear();
        }

        std::string line;

        // Read the column headers (first row of csv)
        if(std::getline(inFile, line)){
            std::stringstream ss(line);
            std::string column;
            while(std::getline(ss, column, ',')){
                columns.push_back(column);
            }
        }

        // Read each row of data        
        while (std::getline(inFile, line)){
            std::stringstream ss(line);
            std::string value;
            std::vector<std::string> row;

            while (std::getline(ss, value, ',')){
                row.push_back(value);
            }

            // Ensure the row has the correcty number of columns
            if(row.size() == columns.size()){
                rows.push_back(row);
            } else {
                std::cout << "Warning: Row has incorrect number of columns and will be skipped." << std::endl;
            }

        }
        inFile.close();  // Close the file
        std::cout << "Table data successfully imported from " << filename << std::endl;
    }

    // single condtion
    // std::vector<std::vector<std::string>> searchRows(const std::string& searchColumn, const std::string& op, const std::string& value){
        // std::lock_guard<std::mutex> lock(tableMutex);  // Lock for concurrency
        // // Find the index of the search column
        // size_t searchIndex = -1;
        // for (size_t i = 0; i < columns.size(); i++) {
        //     if (columns[i] == searchColumn) {
        //         searchIndex = i;
        //         break;
        //     }
        // }
        // if (searchIndex == -1) {
        //     std::cout << "Error: Column '" << searchColumn << "' not found." << std::endl;
        //     return {};
        // }
        // // Vector to store matching rows
        // std::vector<std::vector<std::string>> result;
        // // loop through each row and apply the search condition
        // for(const auto& row : rows){
        //     bool conditionMet = false;
        //     try {
        //         // Convert values to double for numeric comparison
        //         double rowValue = std::stod(row[searchIndex]);
        //         double searchValue = std::stod(value);
        //         if (op == "==") conditionMet = (rowValue == searchValue);
        //         else if (op == "!=") conditionMet = (rowValue != searchValue);
        //         else if (op == "<") conditionMet = (rowValue < searchValue);
        //         else if (op == ">") conditionMet = (rowValue > searchValue);
        //         else if (op == "<=") conditionMet = (rowValue <= searchValue);
        //         else if (op == ">=") conditionMet = (rowValue >= searchValue);
        //     } catch (const std::invalid_argument&) {
        //         // Handle string comparison if values are non-numeric
        //         if (op == "==") conditionMet = (row[searchIndex] == value);
        //         else if (op == "!=") conditionMet = (row[searchIndex] != value);
        //     }
        //     // If the condition is met, add the row to the result
        //     if (conditionMet) {
        //         result.push_back(row);
        //     }
        // }
        // return result;
    // }

    bool evaluateCondition(const Condition& condition, const std::vector<std::string>& row, const std::vector<std::string>& columns) {
        // Find the index of the condition column
        size_t columnIndex = -1;
        for (size_t i = 0; i < columns.size(); i++) {
            if (columns[i] == condition.column) {
                columnIndex = i;
                break;
            }
        }

        // If the column is not found, return false
        if (columnIndex == -1) {
            std::cout << "Error: Column '" << condition.column << "' not found." << std::endl;
            return false;
        }

        // Extract the row value to compare
        const std::string& rowValue = row[columnIndex];
        bool conditionMet = false;

        // Handle numeric and string comparisons
        try {
            double rowValueNumeric = std::stod(rowValue); // Try converting to double for numeric comparison
            double conditionValueNumeric = std::stod(condition.value); // Convert the condition value to double

            // Perform numeric comparisons based on the operator
            if (condition.op == "==") conditionMet = (rowValueNumeric == conditionValueNumeric);
            else if (condition.op == "!=") conditionMet = (rowValueNumeric != conditionValueNumeric);
            else if (condition.op == "<") conditionMet = (rowValueNumeric < conditionValueNumeric);
            else if (condition.op == ">") conditionMet = (rowValueNumeric > conditionValueNumeric);
            else if (condition.op == "<=") conditionMet = (rowValueNumeric <= conditionValueNumeric);
            else if (condition.op == ">=") conditionMet = (rowValueNumeric >= conditionValueNumeric);
        } catch (const std::invalid_argument&) {
            // If conversion fails, assume it's a string comparison
            if (condition.op == "==") conditionMet = (rowValue == condition.value);
            else if (condition.op == "!=") conditionMet = (rowValue != condition.value);
        }

        return conditionMet;
    }


    bool evaluateConditionGroup(const ConditionGroup& group, const std::vector<std::string>& row, const std::vector<std::string>& columns) {
        bool groupResult = (group.logicalOp == "AND");

        // Evaluate each condition in the group
        for (const auto& condition : group.conditions) {
            bool conditionMet = evaluateCondition(condition, row, columns);

            // Apply logical operator
            if (group.logicalOp == "AND") {
                groupResult = groupResult && conditionMet;
                if (!groupResult) break; // Short-circuit for AND
            } else if (group.logicalOp == "OR") {
                groupResult = groupResult || conditionMet;
                if (groupResult) break; // Short-circuit for OR
            }
        }

        // Evaluate each subgroup
        for (const auto& subgroup : group.subgroups) {
            bool subgroupResult = evaluateConditionGroup(subgroup, row, columns);

            // Apply logical operator between groups
            if (group.logicalOp == "AND") {
                groupResult = groupResult && subgroupResult;
                if (!groupResult) break;
            } else if (group.logicalOp == "OR") {
                groupResult = groupResult || subgroupResult;
                if (groupResult) break;
            }
        }

        return groupResult;
    }

    std::vector<std::vector<std::string>> searchRowMultiple(const ConditionGroup& group) {
        std::lock_guard<std::mutex> lock(tableMutex);  // Lock for concurrency

        std::vector<std::vector<std::string>> result;

        for (const auto& row : rows) {
            if (evaluateConditionGroup(group, row, columns)) {
                result.push_back(row);
            }
        }

        return result;
    }

    // search with indexing
    std::vector<std::vector<std::string>> searchRows(const std::vector<Condition>& conditions, const std::string& logicalOp = "AND") {
        std::lock_guard<std::mutex> lock(tableMutex);

        // Vector to store matching rows
        std::vector<std::vector<std::string>> result;
        
        // Check for index-based optimization
        for (const auto& condition : conditions) {
            // Look for an index on the condition's column
            auto indexIt = std::find_if(indexes.begin(), indexes.end(),
                [&condition](const Index& idx) { return idx.column == condition.column; });

            if (indexIt != indexes.end() && condition.op == "==") {
                // Use index for equality-based searches
                auto indexedRows = indexIt->indexMap.find(condition.value);
                if (indexedRows != indexIt->indexMap.end()) {
                    for (const auto& rowIdx : indexedRows->second) {
                        result.push_back(rows[rowIdx]);
                    }
                }
                return result;
            }
        }

        // Fall back to regular multi-condition filtering if no index is found or conditions are more complex
        // (use existing code for multi-condition filtering here)

        return result;
    }

   void createIndex(const std::string& columnName) {
        Index newIndex;
        newIndex.column = columnName;

        // Find the index of the column to be indexed
        size_t columnIndex = -1;
        for (size_t i = 0; i < columns.size(); i++) {
            if (columns[i] == columnName) {
                columnIndex = i;
                break;
            }
        }
        
        if (columnIndex == -1) {
            std::cout << "Error: Column '" << columnName << "' not found." << std::endl;
            return;
        }

        // Populate the index map with column values and row indices
        for (size_t rowIdx = 0; rowIdx < rows.size(); ++rowIdx) {
            std::string key = rows[rowIdx][columnIndex];
            newIndex.indexMap[key].push_back(rowIdx);
        }

        // Add newIndex to the list of indexes
        indexes.push_back(newIndex);
        std::cout << "Index created on column: " << columnName << std::endl;
    }

    bool isValidDataType(const std::string& value, DataType type){
        try
        {
            if(type == DataType::INTEGER){
                std::stoi(value); // check if value can converted to an interger
            } else if (type == DataType::DATE){
                // Check if value matches date format (e.g., YYYY-MM-DD)
                std::regex datePattern("\\d{4}-\\d{2}-\\d{2}");
                return std::regex_match(value, datePattern);
            }
        }
        catch(...)
        {
            return false;
        }
        return true;
        
    }

   // transaction methods
    void beginTransaction(){
        transactions.emplace_back();
        std::cout << "Transaction started." << std::endl;
    };

    void commitTransaction() {
        if (transactions.empty()){
            std::cout << "No active transaction to commit." << std::endl;
            return;
        }

        Transaction& txn = transactions.back();

        // Apply inserts
        for(const auto& row : txn.inserts){
            rows.push_back(row);
        }

        // Apply udpates
        for(const auto& [index, newData] : txn.updates) {
            rows[index] = newData;
        }

        // Apply deleted
        for(auto it = txn.deletes.rbegin(); it != txn.deletes.rend(); ++it){
            rows.erase(rows.begin() + *it);
        }

         transactions.pop_back();
        std::cout << "Transaction committed." << std::endl;
    }

    void rollbackTransaction() {
        if (transactions.empty()) {
            std::cout << "No active transaction to rollback." << std::endl;
            return;
        }

        transactions.back().clear();
        transactions.pop_back();
        std::cout << "Transaction rolled back." << std::endl;
    }

    void addRowTransaction(const std::vector<std::string>& row){
        if(transactions.empty()){
            std::cout<< "Error: No active transaction. Use addRow for non-transactional insert." << std::endl;
            return;
        }
        transactions.back().inserts.push_back(row);
    }

    void updateRowTransaction(size_t rowIndex, const std::vector<std::string>& newData){
        if (transactions.empty()) {
            std::cout << "Error: No active transaction. Use updateRow for non-transactional update." << std::endl;
            return;
        }
        if (rowIndex >= rows.size()) {
            std::cout << "Error: Row index out of range." << std::endl;
            return;
        }
        transactions.back().updates.emplace_back(rowIndex, newData);
    }

    void deleteRowTransaction(size_t rowIndex) {
        if (transactions.empty()) {
            std::cout << "Error: No active transaction. Use deleteRow for non-transactional delete." << std::endl;
            return;
        }
        if (rowIndex >= rows.size()) {
            std::cout << "Error: Row index out of range." << std::endl;
            return;
        }
        transactions.back().deletes.push_back(rowIndex);
    }

    bool evaluateConditionNested(const Condition& condition, const std::vector<std::string>& row, const std::vector<std::string>& columns) {
        if (condition.isGroup) {
            bool groupResult = (condition.logicalOp == "AND");
            for (const auto& subcondition : condition.subconditions) {
                bool subResult = evaluateConditionNested(subcondition, row, columns);

                if (condition.logicalOp == "AND") {
                    groupResult = groupResult && subResult;
                    if (!groupResult) break;  // Short-circuit for AND
                } else if (condition.logicalOp == "OR") {
                    groupResult = groupResult || subResult;
                    if (groupResult) break;  // Short-circuit for OR
                }
            }
            return groupResult;
        } else {
            // Simple condition evaluation
            auto it = std::find(columns.begin(), columns.end(), condition.column);
            if (it == columns.end()) {
                std::cerr << "Error: Column '" << condition.column << "' not found." << std::endl;
                return false;
            }
            size_t columnIndex = std::distance(columns.begin(), it);
            const std::string& cell = row[columnIndex];

            try {
                double cellValue = std::stod(cell);
                double conditionValue = std::stod(condition.value);

                if (condition.op == "==") return cellValue == conditionValue;
                if (condition.op == "!=") return cellValue != conditionValue;
                if (condition.op == "<") return cellValue < conditionValue;
                if (condition.op == ">") return cellValue > conditionValue;
                if (condition.op == "<=") return cellValue <= conditionValue;
                if (condition.op == ">=") return cellValue >= conditionValue;
            } catch (const std::invalid_argument&) {
                // String comparison
                if (condition.op == "==") return cell == condition.value;
                if (condition.op == "!=") return cell != condition.value;
            }
        }
        return false;
    }

    std::vector<std::vector<std::string>> searchRowsCondition(const Condition& condition) {
        std::vector<std::vector<std::string>> result;
        for (const auto& row : rows) {
            if (evaluateConditionNested(condition, row, columns)) {
                result.push_back(row);
            }
        }
        return result;
    }
    
    std::vector<std::vector<std::string>> searchRowsConditionAndOrder(const Condition& condition, const std::vector<OrderBy>& orderByColumns) {
        std::vector<std::vector<std::string>> filteredRows;
        for (const auto& row : rows) {
            if (evaluateConditionNested(condition, row, columns)) {
                filteredRows.push_back(row);
            }
        }

         // Step 2: Sort rows based on orderByColumns
        std::sort(filteredRows.begin(), filteredRows.end(), 
            [&](const std::vector<std::string>& a, const std::vector<std::string>& b) {
                for (const auto& order : orderByColumns) {
                    auto it = std::find(columns.begin(), columns.end(), order.column);
                    if (it == columns.end()) throw std::runtime_error("Invalid column: " + order.column);

                    size_t colIndex = std::distance(columns.begin(), it);

                    if (a[colIndex] != b[colIndex]) { // Compare values
                        if (order.ascending) return a[colIndex] < b[colIndex];
                        else return a[colIndex] > b[colIndex];
                    }
                }
                return false; // Rows are equal
            }
        );

        return filteredRows;
    }

    double aggregate(const std::string& columnName, const std::string& function) const {
        //Find column index
        auto it = std::find(columns.begin(), columns.end(), columnName);
        if(it == columns.end()){
            throw std::runtime_error("Column '" + columnName + "' not found.");
        }

        size_t columnIndex  = std::distance(columns.begin(), it);

        // Ensure column contains numeric 
        std::vector<double> numericValues;
        for(const auto& row : rows){
            try
            {
                numericValues.push_back(std::stod(row[columnIndex]));
            }
            catch(const std::invalid_argument)
            {
                continue;
            }
        }

        if(numericValues.empty()){
            throw std::runtime_error("No numeric values found in column '" + columnName + "'.");
        }

        // Compute aggregate
        if (function == "SUM") {
            return std::accumulate(numericValues.begin(), numericValues.end(), 0.0);
        } else if (function == "AVERAGE") {
            return std::accumulate(numericValues.begin(), numericValues.end(), 0.0) / numericValues.size();
        } else if (function == "MIN") {
            return *std::min_element(numericValues.begin(), numericValues.end());
        } else if (function == "MAX") {
            return *std::max_element(numericValues.begin(), numericValues.end());
        } else {
            throw std::runtime_error("Unsupported aggregate function: " + function);
        }
    }

    std::vector<std::vector<std::string>> groupBy(const std::string& groupColumn, const std::string& aggColumn, const std::string& function) const {
        // Find column indices
        auto groupIt = std::find(columns.begin(), columns.end(), groupColumn);
        auto aggIt = std::find(columns.begin(), columns.end(), aggColumn);

        if (groupIt == columns.end()) {
            throw std::runtime_error("Group column '" + groupColumn + "' not found.");
        }
        if (aggIt == columns.end()) {
            throw std::runtime_error("Aggregate column '" + aggColumn + "' not found.");
        }

        size_t groupIndex = std::distance(columns.begin(), groupIt);
        size_t aggIndex = std::distance(columns.begin(), aggIt);

        // Group rows by group columns 
        std::unordered_map<std::string, std::vector<std::vector<std::string>>> groups;
        for(const auto& row : rows) {
            groups[row[groupIndex]].push_back(row);
        }

        // Compute aggregate for each group
        std::vector<std::vector<std::string>> result;
        result.push_back({groupColumn, function + "(" + aggColumn + ")"});

        for(const auto& [key, groupRows] : groups){
            std::vector<double> numericValues;
            for(const auto& row : groupRows) {
                try
                {
                    numericValues.push_back(std::stod(row[aggIndex]));
                }
                catch(const std::invalid_argument)
                {
                    continue;
                }
            }

            if (numericValues.empty()) continue;

            double aggResult;
            if (function == "SUM") {
                aggResult = std::accumulate(numericValues.begin(), numericValues.end(), 0.0);
            } else if (function == "AVERAGE") {
                aggResult = std::accumulate(numericValues.begin(), numericValues.end(), 0.0) / numericValues.size();
            } else if (function == "MIN") {
                aggResult = *std::min_element(numericValues.begin(), numericValues.end());
            } else if (function == "MAX") {
                aggResult = *std::max_element(numericValues.begin(), numericValues.end());
            } else {
                throw std::runtime_error("Unsupported aggregate function: " + function);
            }

            result.push_back({key, std::to_string(aggResult)});
        }
        return result;
    }
};


int main() {

    // define columns
    std::vector<std::string> columns = {"ID", "Name", "Age", "EnrollmentDate", "Score"};
    std::vector<DataType> studentTypes = {DataType::INTEGER, DataType::STRING, DataType::INTEGER, DataType::DATE, DataType::INTEGER};

    // Create a Table object
    Table studentTable("Students", columns, studentTypes);

    // Add some rows
    // studentTable.addRow({"1", "Alice", "20", "2023-09-01"});
    // studentTable.addRow({"2", "Bob", "22", "2023-09-01"});
    // studentTable.addRow({"3", "Charlie", "19", "2023-09-01"});
    // studentTable.addRow({"4", "Dave", "25", "2023-09-01"});
    // studentTable.addRow({"5", "Eve", "26", "2023-09-01"});  

    // // Save the table to a file
    // studentTable.saveToFile("studentTable.txt");

    // // Clear the table data
    // Table newStudentTable("Students", columns, studentTypes);

    // // Load data from the file
    // newStudentTable.loadFromFile("studentTable.txt");

    // // Display the table loaded from the file
    // std::cout << "Initial table:" << std::endl;
    // newStudentTable.displayTable();

    // tras nstart
    // Start transaction
    studentTable.beginTransaction();

    // Transactional add, update, and delete
    studentTable.addRowTransaction({"1", "sushil", "20", "2023-09-01", "85.5"});
    studentTable.addRowTransaction({"2", "kriti", "22", "2023-09-01", "90.0"});
    studentTable.addRowTransaction({"3", "Charlie", "19", "2023-09-01", "78.0"});
    studentTable.addRowTransaction({"4", "Dave", "25", "2023-09-01", "88.5"});
    studentTable.addRowTransaction({"5", "Eve", "26", "2023-09-01", "86.0"});
    studentTable.addRowTransaction({"6", "Alice", "20", "2023-09-01", "93.0"});
    studentTable.addRowTransaction({"7", "Bob", "22", "2023-09-01", "78.3"});
    // studentTable.updateRowTransaction(0, {"1", "gupta", "21", "2023-09-01"});
    // studentTable.deleteRowTransaction(1);

    // Commit transaction
    studentTable.commitTransaction();

    // Display final table state
    std::cout << "\nTable after committing transaction:" << std::endl;
    studentTable.displayTable();
    // trasn end

    // groupby and agg start
     try {
        auto groupedResults = studentTable.groupBy("Age", "Score", "AVERAGE");

        // Print grouped results
        std::cout << "Group By Results:" << std::endl;
        for (const auto& row : groupedResults) {
            for (const auto& cell : row) {
                std::cout << cell << "\t";
            }
            std::cout << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
    // groupby and agg end

    // aggreration start

    //  try {
    //     std::cout << "Sum of Scores: " << studentTable.aggregate("Score", "SUM") << std::endl;
    //     std::cout << "Average Age: " << studentTable.aggregate("Age", "AVERAGE") << std::endl;
    //     std::cout << "Minimum Score: " << studentTable.aggregate("Score", "MIN") << std::endl;
    //     std::cout << "Maximum Score: " << studentTable.aggregate("Score", "MAX") << std::endl;
    // } catch (const std::exception& e) {
    //     std::cerr << "Error: " << e.what() << std::endl;
    // }

    // aggreration end

    // join table start
    // std::vector<std::string> gradeColumns = {"ID", "Grade"};
    // std::vector<DataType> gradeTypes = {DataType::INTEGER, DataType::STRING};
    // Table gradeTable("Grades", gradeColumns, gradeTypes);

    // gradeTable.beginTransaction();
    // gradeTable.addRowTransaction({"1", "A"});
    // gradeTable.addRowTransaction({"2", "C"});
    // gradeTable.addRowTransaction({"3", "B"});
    // gradeTable.addRowTransaction({"5", "E"});
    // gradeTable.addRowTransaction({"7", "F"});

    // gradeTable.commitTransaction();

    // // Perform JOIN on the "ID" column
    // Table result = studentTable.joinTables(gradeTable, "ID");

    // // Print the result
    // std::cout << "Joined Table:" << std::endl;
    // for (const auto& col : result.columns) {
    //     std::cout << col << "\t";
    // }
    // std::cout << std::endl;

    // for (const auto& row : result.rows) {
    //     for (const auto& cell : row) {
    //         std::cout << cell << "\t";
    //     }
    //     std::cout << std::endl;
    // }
    // join table end

    // search nested condtion  and order start
    // // Define condition
    // Condition condition;
    // condition.isGroup = false;
    // condition.column = "Age";
    // condition.op = ">";
    // condition.value = "20";

    // // Define sorting
    // std::vector<OrderBy> orderByColumns = {
    //     {"Age", true},   // Sort by Age in ascending order
    //     {"Name", false}  // If Age is equal, sort by Name in descending order
    // };

    // // Search and sort
    // auto result = studentTable.searchRowsConditionAndOrder(condition, orderByColumns);

    // std::cout << "\nResult after condition:" << std::endl;
    // // Print result
    // for (const auto& row : result) {
    //     for (const auto& cell : row) {
    //         std::cout << cell << "\t";
    //     }
    //     std::cout << std::endl;
    // }
    // search nested condtion  and order end


    // search nested condtion start
    // Condition condition;
    // condition.isGroup = true;
    // condition.logicalOp = "OR";

    // Condition subCondition1;
    // subCondition1.isGroup = true;
    // subCondition1.logicalOp = "AND";

    // // Create and add first subcondition
    // Condition ageCondition;
    // ageCondition.column = "Age";
    // ageCondition.op = "==";
    // ageCondition.value = "20";
    // subCondition1.subconditions.push_back(ageCondition);

    // // Create and add second subcondition
    // Condition nameCondition;
    // nameCondition.column = "Name";
    // nameCondition.op = "==";
    // nameCondition.value = "Alice";
    // subCondition1.subconditions.push_back(nameCondition);

    // // Add subCondition1 to main condition
    // condition.subconditions.push_back(subCondition1);

    // // Create and add subCondition2 to main condition
    // Condition subCondition2;
    // subCondition2.isGroup = false;
    // subCondition2.column = "Age";
    // subCondition2.op = ">";
    // subCondition2.value = "20";
    // condition.subconditions.push_back(subCondition2);

    // // Search rows with the nested condition
    // std::cout << "\nRows matching the nested condition:" << std::endl;
    // auto result = studentTable.searchRowsCondition(condition);
    // for (const auto& row : result) {
    //     for (const auto& cell : row) {
    //         std::cout << cell << "\t";
    //     }
    //     std::cout << std::endl;
    // }
    // search nested condtion end

    // datatype start
    // Add valid and invalid rows
    // std::cout << "Adding valid row (ID=10, Name=John, Age=20, EnrollmentDate=2023-09-01):" << std::endl;
    // newStudentTable.addRow({"10", "John", "20", "2023-09-01"});
    // std::cout << "After New one Data table:" << std::endl;
    // newStudentTable.displayTable();
    // std::cout << "\nAdding invalid row (ID=abc, Name=Bob, Age=twenty, EnrollmentDate=2023-09-01):" << std::endl;
    // newStudentTable.addRow({"abc", "Jane", "twenty", "2023-09-01"});
    // std::cout << "After New two Data table:" << std::endl;
    // newStudentTable.displayTable();
    // datatype end

    // index start
    // Create an index on the "Age" column
    // studentTable.createIndex("Age");

    // Test search with index
    // std::cout << "\nSearching rows where Age == 20 (using index):" << std::endl;
    // std::vector<Condition> conditions = {{"Age", "==", "20"}};
    // auto result = studentTable.searchRows(conditions);

    // for (const auto& row : result) {
    //     for (const auto& data : row) {
    //         std::cout << data << "\t";
    //     }
    //     std::cout << std::endl;
    // }
    // index end

    // multiple condition group start
    // Define nested condition group
    // ConditionGroup group;
    // group.logicalOp = "OR";

    // ConditionGroup ageAndNameGroup;
    // ageAndNameGroup.logicalOp = "AND";
    // ageAndNameGroup.conditions = {{"Age", ">", "20"}, {"Name", "==", "Alice"}};

    // ConditionGroup ageOrNotNameGroup;
    // ageOrNotNameGroup.logicalOp = "AND";
    // ageOrNotNameGroup.conditions = {{"Age", "<", "18"}};
    // ageOrNotNameGroup.subgroups.push_back({{{"Name", "!=", "Charlie"}}, "AND"});

    // group.subgroups.push_back(ageAndNameGroup);
    // group.subgroups.push_back(ageOrNotNameGroup);

    // auto result = studentTable.searchRowMultiple(group);
    // // Output result for verification
    // for (const auto& row : result) {
    //     for (const auto& data : row) {
    //         std::cout << data << "\t";
    //     }
    //     std::cout << std::endl;
    // }

    // multiple condition group end

    // search by rows select .... where start multiple single condition
    // Test Case 1: Search rows where Age == "20" AND Name == "Alice"
    // std::cout << "\nSearching rows where Age == 20 AND Name == Alice:" << std::endl;
    // std::vector<Condition> conditions1 = {
    //     {"Age", "==", "20"},
    //     {"Name", "==", "Alice"}
    // };
    // std::vector<std::vector<std::string>> result = studentTable.searchRowMultiple(conditions1, "AND");
    // for (const auto& row : result) {
    //     for (const auto& data : row) {
    //         std::cout << data << "\t";
    //     }
    //     std::cout << std::endl;
    // }

    // // Test Case 2: Search rows where Age == "20" OR Name == "Charlie"
    // std::cout << "\nSearching rows where Age == 20 OR Name == Charlie:" << std::endl;
    // std::vector<Condition> conditions2 = {
    //     {"Age", "==", "20"},
    //     {"Name", "==", "Charlie"}
    // };
    // result = studentTable.searchRowMultiple(conditions2, "OR");
    // for (const auto& row : result) {
    //     for (const auto& data : row) {
    //         std::cout << data << "\t";
    //     }
    //     std::cout << std::endl;
    // }
    // search by rows select .... where start multiple single condition

    // search by rows select .... where start single condition
    // Test Case 1: Search rows where Age == "20"
    // std::cout << "\nSearching rows where Age == 20:" << std::endl;
    // std::vector<std::vector<std::string>> result = studentTable.searchRows("Age", "==", "20");
    // for (const auto& row : result) {
    //     for (const auto& data : row) {
    //         std::cout << data << "\t";
    //     }
    //     std::cout << std::endl;
    // }

    // // Test Case 2: Search rows where Name != "Alice"
    // std::cout << "\nSearching rows where Name != 'Alice':" << std::endl;
    // result = studentTable.searchRows("Name", "!=", "Alice");
    // for (const auto& row : result) {
    //     for (const auto& data : row) {
    //         std::cout << data << "\t";
    //     }
    //     std::cout << std::endl;
    // }
    // search by rows select .... where end

    // import from csv start
    // std::string filename = "students.csv";
    // std::cout << "\nImporting table data from " << filename << std::endl;
    // studentTable.importFromCSV(filename);

    // // Display the table after import
    // std::cout << "\nTable contents after import:" << std::endl;
    // studentTable.displayTable();
    // import from csv end

    // export to csv start
    // std::string filename = "students.csv";
    // std::cout << "\nExporting table to " << filename << std::endl;
    // studentTable.exportToCSV(filename);
    // export to csv end

    // delete by conition start
    // Test Case 1: Delete rows where Age == "20"
    // std::cout << "\nDeleting rows where Age == 20:" << std::endl;
    // studentTable.conditionDeleteRow("Age", "==", "20");
    // studentTable.displayTable();

    // // Test Case 2: Delete rows where Name == "Bob"
    // std::cout << "\nDeleting rows where Name == 'Bob':" << std::endl;
    // studentTable.conditionDeleteRow("Name", "==", "Bob");
    // studentTable.displayTable();
    // delte by conition end

    // update by condition start
    // Test Case 1: Update Age to "21" where Age == "20"
    // std::cout << "\nUpdating Age to '21' where Age == 20:" << std::endl;
    // studentTable.conditionUpdateRow("Age", "21", "Age", "==", "20");
    // studentTable.displayTable();

    // // Test Case 2: Update Name to "UpdatedName" where ID == "3"
    // std::cout << "\nUpdating Name to 'UpdatedName' where ID == 3:" << std::endl;
    // studentTable.conditionUpdateRow("Name", "UpdatedName", "ID", "==", "3");
    // studentTable.displayTable();
    // update by conditio end

    // group by start
    
    // Group by "Name" and calculate the average "Age" for each name
    // std::cout << "\nGrouped Aggregation: AVG Age by Name" << std::endl;
    // std::map<std::string, double> avgResult = studentTable.groupedAggregation("Name", "Age", "AVG");

    // for (const auto& [name, avgAge] : avgResult) {
    //     std::cout << "Name: " << name << ", AVG Age: " << avgAge << std::endl;
    // }

    // // Group by "Name" and calculate the SUM of "Age" for each name
    // std::cout << "\nGrouped Aggregation: SUM Age by Name" << std::endl;
    // std::map<std::string, double> sumResult = studentTable.groupedAggregation("Name", "Age", "SUM");

    // for (const auto& [name, sumAge] : sumResult) {
    //     std::cout << "Name: " << name << ", SUM Age: " << sumAge << std::endl;
    // }

    // group by end

    // filters starts

    // Test case 1: Filter rows where Age > 20
    // std::cout << "\nFilter: Age > 20" << std::endl;
    // std::vector<std::vector<std::string>> result = studentTable.filterRows("Age", ">", "20");
    // for (const auto& row : result) {
    //     for (const auto& data : row) {
    //         std::cout << data << "\t";
    //     }
    //     std::cout << std::endl;
    // }

    // // Test case 2: Filter rows where Age == 19
    // std::cout << "\nFilter: Age == 19" << std::endl;
    // result = studentTable.filterRows("Age", "==", "19");
    // for (const auto& row : result) {
    //     for (const auto& data : row) {
    //         std::cout << data << "\t";
    //     }
    //     std::cout << std::endl;
    // }

    // // Test case 3: Filter rows where Name == "Alice"
    // std::cout << "\nFilter: Name == 'Alice'" << std::endl;
    // result = studentTable.filterRows("Name", "==", "Alice");
    // for (const auto& row : result) {
    //     for (const auto& data : row) {
    //         std::cout << data << "\t";
    //     }
    //     std::cout << std::endl;
    // }
    // filters end

    // aggretation start

    // Calculate and display the sum, average, min, and max of the "Age" column
    // std::cout << "\nAggregations on 'Age' column:" << std::endl;
    // std::cout << "Sum: " << studentTable.sumColumn("Age") << std::endl;
    // std::cout << "Average: " << studentTable.avgColumn("Age") << std::endl;
    // std::cout << "Min: " << studentTable.minColumn("Age") << std::endl;
    // std::cout << "Max: " << studentTable.maxColumn("Age") << std::endl;

    // aggretation end

    // double sort start

    // Multi-column sort: Sort by 'Age' in ascending order, and then by 'Name' in descending order
    // std::cout << "\nSorting by 'Age' (ascending) and 'Name' (descending):" << std::endl;
    // studentTable.sortTable({"Name", "Age"}, {true, true});
    // studentTable.displayTable();

    // double sort end

    // single sort start

    // Sort the table by 'Age' in ascending order
    // std::cout << "\nSorting by 'Age' in ascending order:" << std::endl;
    // studentTable.sortTable("Age", true);
    // studentTable.displayTable();

    // // Sort the table by 'Name' in descending order
    // std::cout << "\nSorting by 'Name' in descending order:" << std::endl;
    // studentTable.sortTable("Name", false);
    // studentTable.displayTable();

    // sort end

    // join start

    // First table
    // std::vector<std::string> studentColumns = {"ID", "Name", "Age"};
    // Table studentTable("Students", studentColumns);

    // studentTable.addRow({"1", "Alice", "20"});
    // studentTable.addRow({"2", "Bob", "22"});
    // studentTable.addRow({"3", "Charlie", "19"});

    // // Second table
    // std::vector<std::string> gradeColumns = {"ID", "Grade"};
    // Table gradeTable("Grades", gradeColumns);

    // gradeTable.addRow({"1", "A"});
    // gradeTable.addRow({"2", "B"});
    // gradeTable.addRow({"4", "C"});  // No match in the student table

    // // Display the initial tables
    // std::cout << "Students table:" << std::endl;
    // studentTable.displayTable();

    // std::cout << "Grades table:" << std::endl;
    // gradeTable.displayTable();

    // // Perform the join on the 'ID' column
    // std::cout << "\nJoining Students and Grades tables on 'ID':" << std::endl;
    // std::vector<std::vector<std::string>> joinResult = studentTable.join(gradeTable, "ID");

    // // Display the join result
    // for (const auto& row : joinResult) {
    //     for (const auto& data : row) {
    //         std::cout << data << "\t";
    //     }
    //     std::cout << std::endl;
    // }
    // join end

    // std::cout << "Select Query" << std::endl;

    // // Query: Select rows where ID == "1"
    // std::cout << "Query: Select rows where ID == 1" << std::endl;
    // std::vector<std::vector<std::string>> result = studentTable.selectRows("ID", "1");

    // // Display the result of the query
    // for (const auto& row : result) {
    //     for (const auto& data : row) {
    //         std::cout << data << "\t";
    //     }
    //     std::cout << std::endl;
    // }

    // std::cout << "Update Query" << std::endl;

    // // Update rows: Set Name to "Alicia" where ID == "1"
    // studentTable.updateRows("ID", "1", "Name", "Sushil");
    // studentTable.updateRows("ID", "2", "Name", "Kriti");

    // // Display the table after update
    // studentTable.displayTable();

    // // Delete rows: Remove rows where Age == "22"
    // studentTable.deleteRows("Age", "19");

    // // Display the table after delete
    // studentTable.displayTable();

    // sorting

    // // Display the table before sorting
    // std::cout << "Table before sorting:" << std::endl;
    // studentTable.displayTable();

    // // Sort the rows by Age in ascending order
    // studentTable.sortRows("Age");

    // // Display the table after sorting
    // std::cout << "Table after sorting by Age (ascending):" << std::endl;
    // studentTable.displayTable();

    // // Sort the rows by Name in descending order
    // studentTable.sortRows("Name", false);

    // // Display the table after sorting
    // std::cout << "Table after sorting by Name (descending):" << std::endl;
    // studentTable.displayTable();

    // Aggregate

    // Test aggregation methods
    // std::cout << "\nAggregate Results:" << std::endl;
    // std::cout << "Number of rows: " << studentTable.countRows() << std::endl;
    // std::cout << "Sum of Ages: " << studentTable.sumColumn("Age") << std::endl;
    // std::cout << "Minimum Age: " << studentTable.minColumn("Age") << std::endl;
    // std::cout << "Maximum Age: " << studentTable.maxColumn("Age") << std::endl;
    // std::cout << "Average Age: " << studentTable.averageColumn("Age") << std::endl;


    // Transaction

    // Begin a transaction
    // studentTable.beginTransaction();

    // Perform some operations within the transaction
    // studentTable.addRow({"4", "Dave", "23"});
    // studentTable.deleteRows("ID", "1");

    // Display table after transaction operations
    // std::cout << "Table after transaction operations:" << std::endl;
    // studentTable.displayTable();

    // Rollback the transaction
    // studentTable.rollback();

    // Display table after rollback
    // std::cout << "Table after rollback:" << std::endl;
    // studentTable.displayTable();

    // Begin another transaction and commit it
    // studentTable.beginTransaction();
    // studentTable.addRow({"5", "Eve", "24"});
    // studentTable.commit();

    // Display table after commit
    // std::cout << "Table after commit:" << std::endl;
    // studentTable.displayTable();

    // thread

    // Display the initial table
    // std::cout << "Initial table:" << std::endl;
    // studentTable.displayTable();

    // // Define some operations to be executed concurrently
    // auto addOperation = [&]() { studentTable.addRow({"4", "Dave", "23"}); };
    // // auto updateOperation = [&]() { studentTable.updateRows("ID", "2", "Name", "Robert"); };
    // // auto deleteOperation = [&]() { studentTable.deleteRows("ID", "1"); };

    // // Run the operations concurrently using threads
    // // std::thread thread1(addOperation);
    // // std::thread thread2(updateOperation);
    // // std::thread thread3(deleteOperation);

    // // Wait for all threads to finish
    // // thread1.join();
    // // thread2.join();
    // // thread3.join();

    // // Display the table after concurrent operations
    // // std::cout << "Table after concurrent operations:" << std::endl;
    // // studentTable.displayTable();

    // multi col index start
    // Query using the multi-column index (ID and Age)
    // std::cout << "Query: ID == 1 and Age == 20" << std::endl;
    // std::vector<std::string> result = studentTable.queryByIdAndAge("1", "20");

    // // Display the result of the query
    // if (!result.empty()) {
    //     for (const auto& data : result) {
    //         std::cout << data << "\t";
    //     }
    //     std::cout << std::endl;
    // }

    // multi col index end

    // Save the table to a file
    studentTable.saveToFile("studentTable.txt");

    return 0;
};