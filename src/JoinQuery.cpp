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

//---------------------------------------------------------------------------

std::mutex customerMutex;
std::mutex orderMutex;
std::mutex lineitemMutex;

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
    std::unordered_set<std::string> custkeys;
    std::unordered_map<std::string, std::string> orderToCustkey;
    size_t thread_count = std::thread::hardware_concurrency();
    std::cout << "Thread count: " << thread_count << std::endl;
    customerFile.clear();
    customerFile.seekg(0, std::ios::beg);
    orderFile.clear();
    orderFile.seekg(0, std::ios::beg);
    lineitemFile.clear();
    lineitemFile.seekg(0, std::ios::beg);
    {
        std::vector<std::string> lines;
        std::string line;
        while (std::getline(customerFile, line)) {
            lines.push_back(line);
        }

        size_t totalLines = lines.size();
        size_t chunkSize = totalLines / thread_count;
        std::vector<std::thread> threads;

        for (size_t i = 0; i < thread_count; ++i) {
            size_t start = i * chunkSize;
            size_t end = (i == thread_count - 1) ? totalLines : start + chunkSize;

            threads.emplace_back([&, start, end] {
                for (size_t j = start; j < end; ++j) {
                    std::stringstream ss(lines[j]);
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

        for (auto& thread : threads) {
            thread.join();
        }
    }


    {
        std::vector<std::string> lines;
        std::string line;
        while (std::getline(orderFile, line)) {
            lines.push_back(line);
        }

        size_t totalLines = lines.size();
        size_t chunkSize = totalLines / thread_count;
        std::vector<std::thread> threads;

        for (size_t i = 0; i < thread_count; ++i) {
            size_t start = i * chunkSize;
            size_t end = (i == thread_count - 1) ? totalLines : start + chunkSize;

            threads.emplace_back([&, start, end] {
                for (size_t j = start; j < end; ++j) {
                    std::stringstream ss(lines[j]);
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

        for (auto& thread : threads) {
            thread.join();
        }
    }
    double totalQuantity = 0.0;
    size_t count = 0;
    {
        std::vector<std::string> lines;
        std::string line;
        while (std::getline(lineitemFile, line)) {
            lines.push_back(line);
        }

        size_t totalLines = lines.size();
        size_t chunkSize = totalLines / thread_count;
        std::vector<std::thread> threads;

        for (size_t i = 0; i < thread_count; ++i) {
            size_t start = i * chunkSize;
            size_t end = (i == thread_count - 1) ? totalLines : start + chunkSize;

            threads.emplace_back([&, start, end] {
                for (size_t j = start; j < end; ++j) {
                    std::stringstream ss(lines[j]);
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

        for (auto& thread : threads) {
            thread.join();
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
