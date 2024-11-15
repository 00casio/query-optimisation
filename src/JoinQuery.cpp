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
#include <thread>
#include <mutex>

std::mutex customerMutex;
std::mutex orderMutex;
std::mutex lineitemMutex;

JoinQuery::JoinQuery(std::string lineitemPath, std::string orderPath, std::string customerPath)
    : lineitemFile(lineitemPath), orderFile(orderPath), customerFile(customerPath),
      lineitemPath(lineitemPath), orderPath(orderPath), customerPath(customerPath) {
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
size_t JoinQuery::avg(std::string segmentParam)
{
    std::unordered_set<std::string> custkeys;
    std::unordered_map<std::string, std::string> orderToCustkey;
    size_t thread_count = std::thread::hardware_concurrency();
    std::cout << "Thread count: " << thread_count << std::endl;

    size_t customerFileSize = customerFile.seekg(0, std::ios::end).tellg();
    size_t customerChunkSize = customerFileSize / thread_count;

    std::vector<std::thread> customerThreads;
    for (size_t i = 0; i < thread_count; ++i) {
        customerThreads.emplace_back([&, i, customerPath = this->customerPath] {
            std::ifstream customerFileThread(customerPath);
            size_t start = i * customerChunkSize;
            customerFileThread.seekg(start);
            if (i > 0) {
                char c;
                while (customerFileThread.get(c) && c != '\n') {
                    start++;
                }
            }
            size_t end = (i == thread_count - 1) ? customerFileSize : (i + 1) * customerChunkSize;
            std::string line;
            while (customerFileThread.tellg() < end && std::getline(customerFileThread, line)) {
                std::stringstream ss(line);
                std::string item;
                std::vector<std::string> columns;
                while (std::getline(ss, item, '|')) {
                    columns.push_back(item);
                }
                if (columns.size() > 6 && columns[6] == segmentParam) {
                    std::lock_guard<std::mutex> lock(customerMutex);
                    custkeys.insert(columns[0]);
                }
            }
        });
    }

    for (auto& thread : customerThreads) {
        thread.join();
    }

    size_t orderFileSize = orderFile.seekg(0, std::ios::end).tellg();
    size_t orderChunkSize = orderFileSize / thread_count;

    std::vector<std::thread> orderThreads;
    for (size_t i = 0; i < thread_count; ++i) {
        orderThreads.emplace_back([&, i, orderPath = this->orderPath] {
            std::ifstream orderFileThread(orderPath);
            size_t start = i * orderChunkSize;
            orderFileThread.seekg(start);
            if (i > 0) {
                char c;
                while (orderFileThread.get(c) && c != '\n') {
                    start++;
                }
            }
            size_t end = (i == thread_count - 1) ? orderFileSize : (i + 1) * orderChunkSize;
            std::string line;
            while (orderFileThread.tellg() < end && std::getline(orderFileThread, line)) {
                std::stringstream ss(line);
                std::string item;
                std::vector<std::string> columns;
                while (std::getline(ss, item, '|')) {
                    columns.push_back(item);
                }
                if (columns.size() > 1 && custkeys.find(columns[1]) != custkeys.end()) {
                    std::lock_guard<std::mutex> lock(orderMutex);
                    orderToCustkey[columns[0]] = columns[1];
                }
            }
        });
    }

    for (auto& thread : orderThreads) {
        thread.join();
    }

    size_t lineitemFileSize = lineitemFile.seekg(0, std::ios::end).tellg();
    size_t lineitemChunkSize = lineitemFileSize / thread_count;

    double totalQuantity = 0.0;
    size_t count = 0;
    std::vector<std::thread> lineitemThreads;
    for (size_t i = 0; i < thread_count; ++i) {
        lineitemThreads.emplace_back([&, i, lineitemPath = this->lineitemPath] {
            std::ifstream lineitemFileThread(lineitemPath);
            size_t start = i * lineitemChunkSize;
            lineitemFileThread.seekg(start);
            if (i > 0) {
                char c;
                while (lineitemFileThread.get(c) && c != '\n') {
                    start++;
                }
            }
            size_t end = (i == thread_count - 1) ? lineitemFileSize : (i + 1) * lineitemChunkSize;
            std::string line;
            while (lineitemFileThread.tellg() < end && std::getline(lineitemFileThread, line)) {
                std::stringstream ss(line);
                std::string item;
                std::vector<std::string> columns;
                while (std::getline(ss, item, '|')) {
                    columns.push_back(item);
                }
                if (columns.size() > 1 && orderToCustkey.find(columns[0]) != orderToCustkey.end()) {
                    std::lock_guard<std::mutex> lock(lineitemMutex);
                    totalQuantity += std::stod(columns[4]);
                    count++;
                }
            }
        });
    }

    for (auto& thread : lineitemThreads) {
        thread.join();
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