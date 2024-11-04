#include <iostream>
#include <fstream>
#include <string>
#include <algorithm>

struct Node {
    std::string key;
    std::string value;
    int height;
    Node *left, *right;

    Node(std::string k, std::string v) : key(k), value(v), left(nullptr), right(nullptr) {
        height = 1;
    }
};

// Function Declarations (Forward Declarations)
Node* rightRotate(Node* y);
Node* leftRotate(Node* x);
int height(Node* N);
int getBalance(Node* N);
void rangeQuery(Node* root, const std::string& low, const std::string& high);

// A utility function to get the height of the tree
int height(Node *N) {
    if (N == nullptr)
        return 0;
    return N->height;
}

// Get balance factor of node N
int getBalance(Node *N) {
    if (N == nullptr)
        return 0;
    return height(N->left) - height(N->right);
}

// range query
void rangeQuery(Node* root, const std::string& low, const std::string& high) {
    if (root == nullptr)
        return;

    // Traverse left subtree if current node's key is greater than low
    if (low < root->key)
        rangeQuery(root->left, low, high);

    // If current node's key is within range, print the key-value pair
    if (low <= root->key && root->key <= high)
        std::cout << root->key << ": " << root->value << std::endl;

    // Traverse right subtree if current node's key is less than high
    if (root->key < high)
        rangeQuery(root->right, low, high);
}

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
    root->height = 1 + std::max(height(root->left), height(root->right));

    // Get the balance factor of this ancestor node
    int balance = getBalance(root);

    // If this node becomes unbalanced,
    // then there are 4 cases

    // Left Left Case
    if (balance > 1 && key < root->left->key)
        return rightRotate(root);

    // Right Right Case
    if (balance < -1 && key > root->right->key)
        return leftRotate(root);

    // Left Right Case
    if (balance > 1 && key > root->left->key) {
        root->left = leftRotate(root->left);
        return rightRotate(root);
    }

    // Right Left Case
    if (balance < -1 && key < root->right->key) {
        root->right = rightRotate(root->right);
        return leftRotate(root);
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

// Right Rotation
Node* rightRotate(Node* y) {
    Node* x = y->left;
    Node* T2 = x->right;

    // Perform rotation
    x->right = y;
    y->left = T2;

    // Update heights
    y->height = std::max(height(y->left), height(y->right)) + 1;
    x->height = std::max(height(x->left), height(x->right)) + 1;

    // Return new root
    return x;
}

// Left Rotation
Node* leftRotate(Node* x) {
    Node* y = x->right;
    Node* T2 = y->left;

    // Perform rotation
    y->left = x;
    x->right = T2;

    // Update heights
    x->height = std::max(height(x->left), height(x->right)) + 1;
    y->height = std::max(height(y->left), height(y->right)) + 1;

    // Return new root
    return y;
}

// Deletion
Node* deleteNode(Node* root, std::string key) {
    if (root == nullptr) {
        return root;
    }

    if (key < root->key) {
        root->left = deleteNode(root->left, key);
    } else if (key > root->key) {
        root->right = deleteNode(root->right, key);
    } else {
        // Node to be deleted found

        // Case 1: Node with only one child or no child
        if (root->left == nullptr || root->right == nullptr) {
            Node* temp = (root->left) ? root->left : root->right;

            if (temp == nullptr) {
                // No child case
                temp = root;
                root = nullptr;
            } else {
                // One child case
                *root = *temp;
            }

            delete temp;
        } else {
            // Node with two children, get the in-order successor (smallest in the right subtree)
            Node* temp = findMin(root->right);

            // Copy the in-order successor's data to this node
            root->key = temp->key;
            root->value = temp->value;

            // Delete the in-order successor
            root->right = deleteNode(root->right, temp->key);
        }
    }

    // If the tree had only one node, return
    if (root == nullptr)
        return root;

    // Update height of the current node
    root->height = 1 + std::max(height(root->left), height(root->right));

    // Get the balance factor of this node
    int balance = getBalance(root);

    // Rebalance the tree

    // Left Left Case
    if (balance > 1 && getBalance(root->left) >= 0)
        return rightRotate(root);

    // Left Right Case
    if (balance > 1 && getBalance(root->left) < 0) {
        root->left = leftRotate(root->left);
        return rightRotate(root);
    }

    // Right Right Case
    if (balance < -1 && getBalance(root->right) <= 0)
        return leftRotate(root);

    // Right Left Case
    if (balance < -1 && getBalance(root->right) > 0) {
        root->right = rightRotate(root->right);
        return leftRotate(root);
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

void prefixSearch(Node* root, const std::string& prefix) {
    if (root == nullptr)
        return;

    // In-order traversal to visit nodes in sorted order
    prefixSearch(root->left, prefix);

    // Check if the current node's key starts with the prefix
    if (root->key.substr(0, prefix.size()) == prefix)
        std::cout << root->key << ": " << root->value << std::endl;

    prefixSearch(root->right, prefix);
}

int main() {
    Node* root = nullptr;
    int choice;
    std::string key, value, rangeHigh, rangeLow, prefix;
    std::string filename = "store.txt";

    // Load the data from the file at the beginning
    loadFromFile(root, filename);

    do {
        std::cout << "Menu:\n1. Add Key-Value\n2. Get Value by Key\n3. Delete Key-Value\n4. Print All\n5. Range Query\n6. Search for prefix\n10. Exit\n";
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
                std::cout << "In-order traversal of the AVL Tree:" << std::endl;
                inorder(root);
                break;
            
            case 5:
                std::cout << "Enter the key low range: ";
                std::cin >> rangeLow;
                std::cout << "Enter the key high range: ";
                std::cin >> rangeHigh;
                std::cout << "Performing Range Query (Keys between " << rangeLow << " and " << rangeHigh << "):" << std::endl;
                rangeQuery(root, rangeLow, rangeHigh);
                break;

            case 6:
                std::cout << "Enter Prefix:";
                std::cin >> prefix;
                 std::cout << "Keys that start with '" << prefix << "':" << std::endl;
                prefixSearch(root, prefix);
                break;

            case 10:
                std::cout << "Exiting and saving to file...\n";
                break;

            default:
                std::cout << "Invalid choice, try again.\n";
        }

    } while (choice != 10);

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