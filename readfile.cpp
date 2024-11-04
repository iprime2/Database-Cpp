#include <iostream>
#include <fstream>
#include <string>

int main() {

    // writing file
    std::ofstream outputFile("test.txt", std::ios::app); // appendmode
    if(outputFile.is_open()){
        outputFile << "\nNew data added!" << std::endl;
        outputFile.close();
    } else {
        std::cout << "Unable to open file." << std::endl;
    }
    
    // reading file
    std::ifstream inputFile("test.txt");
    std::string line;

    if(inputFile.is_open()){
        while(getline(inputFile, line)){
            std::cout<< line << std::endl;
        }
        inputFile.close();
    }else{
        std::cout<< "Unable to open file."<< std::endl;
    }



    return 0;
}