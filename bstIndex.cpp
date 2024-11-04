#include <iostream>
#include <fstream>
#include <string>

struct Node {
    std::string key;
    std::string value;
    Node *left, *right;

    Node(std::string k, std::string v) : key(k), value(v), left(nullptr), right(nullptr) {}
};

// Insert the data
Node* insert(Node* root, std::string key, std::string value) {
    if (root == nullptr) {
        return new Node(key, value);
    }

    if (key < root->key) {
        root->left = insert(root->left, key, value);
    } else if (key > root->key) {
        root->right = insert(root->right, key, value);
    } else {
        root->value = value;  // Update value if key already exists
    }
    return root;
}

// Search data in the map using BST
std::string search(Node* root, std::string key) {
    if (root == nullptr || root->key == key) {
        return (root == nullptr) ? "Key not found" : root->value;
    }

    if (key < root->key) {
        return search(root->left, key);
    }
    return search(root->right, key);
}

// Find the node with the smallest key (in-order successor)
Node* findMin(Node* root) {
    while (root->left != nullptr) {
        root = root->left;
    }
    return root;
}

// Delete a node from the BST
Node* deleteNode(Node* root, std::string key) {
    if (root == nullptr) {
        return root;
    }

    if (key < root->key) {
        root->left = deleteNode(root->left, key);
    } else if (key > root->key) {
        root->right = deleteNode(root->right, key);
    } else {
        // Case 1: Node with only one child or no child
        if (root->left == nullptr) {
            Node* temp = root->right;
            delete root;
            return temp;
        } else if (root->right == nullptr) {
            Node* temp = root->left;
            delete root;
            return temp;
        }

        // Case 2: Node with two children, find in-order successor
        Node* temp = findMin(root->right);

        // Replace the root's key-value with the successor's key-value
        root->key = temp->key;
        root->value = temp->value;

        // Delete the in-order successor
        root->right = deleteNode(root->right, temp->key);
    }
    return root;
}

// In-order traversal for debugging purposes
void inorder(Node* root) {
    if (root != nullptr) {
        inorder(root->left);
        std::cout << root->key << ": " << root->value << std::endl;
        inorder(root->right);
    }
}

// Function to load key-value pairs from a file
void loadFromFile(Node*& root, const std::string& filename) {
    std::ifstream inputFile(filename);
    std::string key, value;
    
    if (inputFile.is_open()) {
        while (inputFile >> key >> value) {
            root = insert(root, key, value);
        }
        inputFile.close();
    } else {
        std::cout << "File not found. Starting with an empty tree." << std::endl;
    }
}

// Function to save key-value pairs to a file
void saveToFile(Node* root, std::ofstream& outputFile) {
    if (root == nullptr) return;

    saveToFile(root->left, outputFile);
    outputFile << root->key << " " << root->value << std::endl;
    saveToFile(root->right, outputFile);
}

int main() {
    Node* root = nullptr;
    int choice;
    std::string key, value;
    std::string filename = "store.txt";

    // Load the data from the file at the beginning
    loadFromFile(root, filename);

    do {
        std::cout << "Menu:\n1. Add Key-Value\n2. Get Value by Key\n3. Delete Key-Value\n4.Print All\n5. Exit\n";
        std::cin >> choice;

        switch (choice) {
            case 1:
                std::cout << "Enter key: ";
                std::cin >> key;
                std::cout << "Enter value: ";
                std::cin >> value;
                root = insert(root, key, value);
                std::cout << "Key-Value added.\n";
                break;

            case 2:
                std::cout << "Enter key: ";
                std::cin >> key;
                std::cout << "Value for " << key << ": " << search(root, key) << std::endl;
                break;

            case 3:
                std::cout << "Enter key to delete: ";
                std::cin >> key;
                root = deleteNode(root, key);
                std::cout << "Key-Value deleted.\n";
                break;
            
            case 4:
                std::cout << "In-order traversal of the BST:" << std::endl;
                inorder(root);
                break;

            case 5:
                std::cout << "Exiting and saving to file...\n";
                break;

            default:
                std::cout << "Invalid choice, try again.\n";
        }

    } while (choice != 5);

    // Save the data to the file before exiting
    std::ofstream outputFile(filename);
    if (outputFile.is_open()) {
        saveToFile(root, outputFile);
        std::cout << "Data saved to file!\n";
        outputFile.close();
    } else {
        std::cout << "Error: Could not open file for saving.\n";
    }

    return 0;
}
