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
#include <mutex>

std::mutex customerMutex;
std::mutex orderMutex;
std::mutex lineitemMutex;

JoinQuery::JoinQuery(std::string lineitemPath, std::string orderPath, std::string customerPath)
    : lineitemPath(lineitemPath), orderPath(orderPath), customerPath(customerPath),
      lineitemFile(lineitemPath), orderFile(orderPath), customerFile(customerPath) {
    if (!lineitemFile.is_open()) {
        std::cerr << "Error: Unable to open lineitem file: " << lineitemPath << "\n";
        exit(1);
    }

    if (!orderFile.is_open()) {
        std::cerr << "Error: Unable to open orders file: " << orderPath << "\n";
        exit(1);
    }

    if (!customerFile.is_open()) {
        std::cerr << "Error: Unable to open customer file: " << customerPath << "\n";
        exit(1);
    }
}

size_t JoinQuery::avg(std::string segmentParam)
{
    std::unordered_set<std::string> custkeys;
    std::unordered_map<std::string, std::string> orderToCustkey;

    // Process the customer file
    customerFile.clear();
    customerFile.seekg(0, std::ios::beg);
    std::string line;
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

    orderFile.clear();
    orderFile.seekg(0, std::ios::beg);
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

    lineitemFile.clear();
    lineitemFile.seekg(0, std::ios::beg);
    double totalQuantity = 0.0;
    size_t count = 0;
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

size_t JoinQuery::lineCount(std::string rel)
{
   std::ifstream relation(rel);
   assert(relation);  // make sure the provided string references a file
   size_t n = 0;
   for (std::string line; std::getline(relation, line);) n++;
   return n;
}