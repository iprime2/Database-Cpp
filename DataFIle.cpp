#include <iostream>
#include <map>
#include <fstream>
#include <string>

// Function to load key-value pairs from a file
void loadFromFile(std::map<std::string, std::string> &keyValueStore, const std::string &filename) {
    std::ifstream inputFile(filename);
    std::string key, value;
    
    if (inputFile.is_open()) {
        while (inputFile >> key >> value) {
            keyValueStore[key] = value;
        }
        inputFile.close();
    }
}

// Function to save key-value pairs to a file
void saveToFile(const std::map<std::string, std::string> &keyValueStore, const std::string &filename) {
    std::ofstream outputFile(filename);
    
    if (outputFile.is_open()) {
        for (const auto &pair : keyValueStore) {
            outputFile << pair.first << " " << pair.second << std::endl;
        }
        outputFile.close();
    }
}

int main() {
    std::map<std::string, std::string> keyValueStore;
    std::string filename = "store.txt";

    // Load existing data from file
    loadFromFile(keyValueStore, filename);

    int choice;
    std::string key, value;

    do {
        std::cout << "Menu:\n1. Add Key-Value\n2. Get Value by Key\n3. Delete Key-Value\n4. Exit\n";
        std::cin >> choice;

        switch (choice) {
            case 1:
                std::cout << "Enter key: ";
                std::cin >> key;
                std::cout << "Enter value: ";
                std::cin >> value;
                keyValueStore[key] = value;
                saveToFile(keyValueStore, filename);
                std::cout << "Key-Value added.\n";
                break;

            case 2:
                std::cout << "Enter key: ";
                std::cin >> key;
                if (keyValueStore.find(key) != keyValueStore.end()) {
                    std::cout << "Value: " << keyValueStore[key] << std::endl;
                } else {
                    std::cout << "Key not found.\n";
                }
                break;

            case 3:
                std::cout << "Enter key to delete: ";
                std::cin >> key;
                if (keyValueStore.erase(key)) {
                    saveToFile(keyValueStore, filename);
                    std::cout << "Key-Value deleted.\n";
                } else {
                    std::cout << "Key not found.\n";
                }
                break;

            case 4:
                std::cout << "Exiting...\n";
                break;

            default:
                std::cout << "Invalid choice, try again.\n";
        }

    } while (choice != 4);

    return 0;
}
