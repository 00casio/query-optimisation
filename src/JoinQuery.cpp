#include "JoinQuery.hpp"
#include <assert.h>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <iostream>
#include <cstdlib>
#include <unordered_set>




//---------------------------------------------------------------------------

JoinQuery::JoinQuery(std::string lineitemPath, std::string orderPath, std::string customerPath)
    : lineitemFile(lineitemPath), orderFile(orderPath), customerFile(customerPath) {
    if (!lineitemFile.is_open()) {
        std::cerr << "Error: Unable to open lineitem file.\n";
        exit(1);
    }

    if (!orderFile.is_open()) {
        std::cerr << "Error: Unable to open orders file.\n";
        exit(1);
    }

    if (!customerFile.is_open()) {
        std::cerr << "Error: Unable to open customer file.\n";
        exit(1);
    }
}
//---------------------------------------------------------------------------
size_t JoinQuery::avg(std::string segmentParam)
{
    std::string line;
    std::unordered_set<std::string> custkeys;
    std::unordered_map<std::string, std::string> orderToCustkey;
    customerFile.clear();
    customerFile.seekg(0, std::ios::beg);
    orderFile.clear();
    orderFile.seekg(0, std::ios::beg);
    lineitemFile.clear();
    lineitemFile.seekg(0, std::ios::beg);


    while (std::getline(customerFile, line)) {
        std::stringstream ss(line);
        std::string item;
        std::vector<std::string> columns;

        while (std::getline(ss, item, '|')) {
            columns.push_back(item);
        }

   
        if (columns.size() > 6 && columns[6] == segmentParam) {
            custkeys.insert(columns[0]); 
        }
    }


    while (std::getline(orderFile, line)) {
        std::stringstream ss(line);
        std::string item;
        std::vector<std::string> columns;

        while (std::getline(ss, item, '|')) {
            columns.push_back(item);
        }

        if (columns.size() > 1 && custkeys.find(columns[1]) != custkeys.end()) {
            orderToCustkey[columns[0]] = columns[1]; 
        }
    }

    size_t count = 0;
    double totalQuantity = 0.0;

    while (std::getline(lineitemFile, line)) {
        std::stringstream ss(line);
        std::string item;
        std::vector<std::string> columns;

        while (std::getline(ss, item, '|')) {
            columns.push_back(item);
        }

        if (columns.size() > 1 && orderToCustkey.find(columns[0]) != orderToCustkey.end()) {
            totalQuantity += std::stod(columns[4]);
            count++;
        }
    }

    if (count == 0) return 0;
    return static_cast<size_t>((totalQuantity / count) * 100);
}
//---------------------------------------------------------------------------
size_t JoinQuery::lineCount(std::string rel)
{
   std::ifstream relation(rel);
   assert(relation);  // make sure the provided string references a file
   size_t n = 0;
   for (std::string line; std::getline(relation, line);) n++;
   return n;
}
//---------------------------------------------------------------------------
