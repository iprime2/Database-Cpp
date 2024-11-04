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

// Custom hash function for tuples
struct TupleHasher {
    template <typename T>
    std::size_t operator()(const T& tuple) const {
        auto hash1 = std::hash<std::tuple_element_t<0, T>>()(std::get<0>(tuple));
        auto hash2 = std::hash<std::tuple_element_t<1, T>>()(std::get<1>(tuple));
        return hash1 ^ (hash2 << 1);  // Combine the hashes
    }
};

class Table {
private:
    std::string tableName;
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;

    std::unordered_map<std::string, size_t> index;
    // Multi-column index for faster querying based on multiple columns
    std::unordered_map<std::tuple<std::string, std::string>, size_t, TupleHasher> multiColumnIndex;
    bool inTransaction = false;
    std::vector<std::vector<std::string>> transactionBackup;

    // Lock for concurrency control
    std::mutex tableMutex;

public:
    //constructor
    Table(const std::string& name, const std::vector<std::string>& cols)
        : tableName(name), columns(cols) {
        std::cout << "Table '" << tableName << "' created with columns: ";
        for (const auto& col : columns) {
            std::cout << col << " ";
        }
        std::cout << std::endl;
    }

    // Add a row of data
    void addRow(std::vector<std::string> rowData) {
        std::lock_guard<std::mutex> lock(tableMutex);  // Lock for concurrency

        if (rowData.size() != columns.size()) {
            std::cout << "Error: Row size (" << rowData.size() << ") does not match the number of columns (" << columns.size() << ")." << std::endl;
            return;
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

    // transaction methods
    void beginTransaction(){
        if (inTransaction) {
            std::cout << "Transaction already in progress!" << std::endl;
            return;
        }
        inTransaction = true;
        transactionBackup = rows;  // Backup the current rows for rollback
        std::cout << "Transaction started." << std::endl;
    };

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


};


int main() {

    // define columns
    std::vector<std::string> columns = {"ID", "Name", "Age"};

    // Create a Table object
    Table studentTable("Students", columns);

    // Add some rows
    studentTable.addRow({"1", "Alice", "20"});
    studentTable.addRow({"2", "Bob", "22"});
    studentTable.addRow({"3", "Charlie", "19"});
    studentTable.addRow({"4", "Dave", "25"});
    studentTable.addRow({"5", "Eve", "invalid"});  

    // Save the table to a file
    studentTable.saveToFile("studentTable.txt");

    // Clear the table data
    Table newStudentTable("Students", columns);

    // Load data from the file
    newStudentTable.loadFromFile("studentTable.txt");

    // Display the table loaded from the file
    std::cout << "Initial table:" << std::endl;
    newStudentTable.displayTable();

    // aggretation start

    // Calculate and display the sum, average, min, and max of the "Age" column
    std::cout << "\nAggregations on 'Age' column:" << std::endl;
    std::cout << "Sum: " << studentTable.sumColumn("Age") << std::endl;
    std::cout << "Average: " << studentTable.avgColumn("Age") << std::endl;
    std::cout << "Min: " << studentTable.minColumn("Age") << std::endl;
    std::cout << "Max: " << studentTable.maxColumn("Age") << std::endl;

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
    // auto updateOperation = [&]() { studentTable.updateRows("ID", "2", "Name", "Robert"); };
    // auto deleteOperation = [&]() { studentTable.deleteRows("ID", "1"); };

    // // Run the operations concurrently using threads
    // std::thread thread1(addOperation);
    // std::thread thread2(updateOperation);
    // std::thread thread3(deleteOperation);

    // // Wait for all threads to finish
    // thread1.join();
    // thread2.join();
    // thread3.join();

    // // Display the table after concurrent operations
    // std::cout << "Table after concurrent operations:" << std::endl;
    // studentTable.displayTable();

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

    return 0;
};