#include <iostream>
#include <map>
#include <string>

int main() {
    std::map<std::string, std::string> keyValueStore;
    int choice;
    std::string key, value;

    do{
        std::cout << "Menu:\n1. Add Key-Value\n2. Get Value by Key\n3. Delete Key-Value\n4. Exit\n";
        std::cin >> choice;

        switch (choice)
        {
        case 1:
            std::cout << "Enter key: ";
            std::cin >> key;
            std::cout << "Enter value: ";
            std::cin >> value;
            keyValueStore[key] = value;
            break;

        case 2:
            std::cout << "Enter key: ";
            std::cin >> key;
            if(keyValueStore.find(key) != keyValueStore.end()){
                std::cout << "Value: " << keyValueStore[key] << std::endl;
            } else {
                std::cout << "Key not found.\n";
            }
            break;
        
        case 3:
            std::cout << "Enter key to delete: ";
            std::cin >> key;
            if(keyValueStore.erase(key)){
                std::cout << "Key-Value deletes.\n ";
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
    } while (choice !=0 );

    return 0;
}