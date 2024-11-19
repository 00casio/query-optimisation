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
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <immintrin.h>

using namespace std;

class FileView {
    int handle;
    char* data;
    size_t size;

public:
    FileView(const string& fileName) {
        handle = open(fileName.c_str(), O_RDONLY);
        if (handle < 0) exit(1);
        lseek(handle, 0, SEEK_END);
        size = lseek(handle, 0, SEEK_CUR);
        data = static_cast<char*>(mmap(nullptr, size, PROT_READ, MAP_SHARED, handle, 0));
    }
    ~FileView() {
        munmap(data, size);
        close(handle);
    }
    const char* begin() const { return data; }
    const char* end() const { return data + size; }
    size_t getSize() const { return size; }
};

static const char* findNl(const char* iter, const char* limit) {
    auto pattern = _mm256_set1_epi8('\n');
    while (iter + 32 <= limit) {
        auto block = _mm256_lddqu_si256(reinterpret_cast<const __m256i*>(iter));
        auto foundPattern = _mm256_cmpeq_epi8(block, pattern);
        uint64_t matches = _mm256_movemask_epi8(foundPattern);

        if (matches != 0)
            return iter + (__builtin_ctzll(matches));
        iter += 32;
    }
    while ((iter < limit) && (iter[0] != '\n')) ++iter;
    return iter;
}

static const char* findBars(const char* iter, const char* limit, unsigned n) {
    auto pattern = _mm256_set1_epi8('|');
    while (iter + 32 <= limit) {
        auto block = _mm256_lddqu_si256(reinterpret_cast<const __m256i*>(iter));
        auto foundPattern = _mm256_cmpeq_epi8(block, pattern);
        uint64_t matches = _mm256_movemask_epi8(foundPattern);

        if (matches != 0) {
            unsigned hits = __builtin_popcount(matches);
            if (hits < n) {
                n -= hits;
            } else {
                for (; n > 1; n--) matches &= matches - 1;
                return iter + (__builtin_ctzll(matches));
            }
        }
        iter += 32;
    }
    while (iter < limit) {
        if (iter[0] == '|') {
            if (!--n) return iter;
        }
        ++iter;
    }
    return iter;
}

std::mutex customerMutex;
std::mutex orderMutex;
std::mutex lineitemMutex;

JoinQuery::JoinQuery(std::string lineitemPath, std::string orderPath, std::string customerPath)
    : lineitemPath(lineitemPath), orderPath(orderPath), customerPath(customerPath) {}

size_t JoinQuery::avg(std::string segmentParam)
{
    std::unordered_set<std::string> custkeys;
    std::unordered_map<std::string, std::string> orderToCustkey;
    size_t thread_count = 3;

    FileView customerFile(customerPath);
    FileView orderFile(orderPath);
    FileView lineitemFile(lineitemPath);

    size_t customerFileSize = customerFile.getSize();
    size_t customerChunkSize = customerFileSize / thread_count;

    std::vector<std::thread> customerThreads;
    for (size_t i = 0; i < thread_count; ++i) {
        customerThreads.emplace_back([&, i, segmentParam] {
            size_t start = i * customerChunkSize;
            const char* customerData = customerFile.begin() + start;
            if (i > 0) {
                while (customerData < customerFile.end() && *customerData != '\n') {
                    ++customerData;
                }
                ++customerData;
            }
            size_t end = (i == thread_count - 1) ? customerFileSize : (i + 1) * customerChunkSize;
            std::unordered_set<std::string> localCustkeys;
            const char* customerEnd = customerFile.begin() + end;
            while (customerData < customerEnd) {
                const char* lineEnd = findNl(customerData, customerEnd);
                const char* custkeyEnd = findBars(customerData, lineEnd, 1);
                const char* segmentStart = findBars(customerData, lineEnd, 6) + 1;
                const char* segmentEnd = findBars(segmentStart, lineEnd, 1);
                std::string segment(segmentStart, segmentEnd);
                if (segment == segmentParam) {
                    localCustkeys.insert(std::string(customerData, custkeyEnd));
                }
                customerData = lineEnd + 1;
            }
            {
                std::lock_guard<std::mutex> lock(customerMutex);
                custkeys.insert(localCustkeys.begin(), localCustkeys.end());
            }
        });
    }

    for (auto& thread : customerThreads) {
        thread.join();
    }

    // Process the order file in parallel
    size_t orderFileSize = orderFile.getSize();
    size_t orderChunkSize = orderFileSize / thread_count;

    std::vector<std::thread> orderThreads;
    for (size_t i = 0; i < thread_count; ++i) {
        orderThreads.emplace_back([&, i] {
            size_t start = i * orderChunkSize;
            const char* orderData = orderFile.begin() + start;
            if (i > 0) {
                while (orderData < orderFile.end() && *orderData != '\n') {
                    ++orderData;
                }
                ++orderData;
            }
            size_t end = (i == thread_count - 1) ? orderFileSize : (i + 1) * orderChunkSize;
            std::unordered_map<std::string, std::string> localOrderToCustkey;
            const char* orderEnd = orderFile.begin() + end;
            while (orderData < orderEnd) {
                const char* lineEnd = findNl(orderData, orderEnd);
                const char* orderkeyEnd = findBars(orderData, lineEnd, 1);
                const char* custkeyStart = orderkeyEnd + 1;
                const char* custkeyEnd = findBars(custkeyStart, lineEnd, 1);
                if (custkeys.find(std::string(custkeyStart, custkeyEnd)) != custkeys.end()) {
                    localOrderToCustkey[std::string(orderData, orderkeyEnd)] = std::string(custkeyStart, custkeyEnd);
                }
                orderData = lineEnd + 1;
            }
            {
                std::lock_guard<std::mutex> lock(orderMutex);
                orderToCustkey.insert(localOrderToCustkey.begin(), localOrderToCustkey.end());
            }
        });
    }

    for (auto& thread : orderThreads) {
        thread.join();
    }

    size_t lineitemFileSize = lineitemFile.getSize();
    size_t lineitemChunkSize = lineitemFileSize / thread_count;

    double totalQuantity = 0.0;
    size_t count = 0;
    std::vector<std::thread> lineitemThreads;
    for (size_t i = 0; i < thread_count; ++i) {
        lineitemThreads.emplace_back([&, i] {
            size_t start = i * lineitemChunkSize;
            const char* lineitemData = lineitemFile.begin() + start;
            if (i > 0) {
                while (lineitemData < lineitemFile.end() && *lineitemData != '\n') {
                    ++lineitemData;
                }
                ++lineitemData;
            }
            size_t end = (i == thread_count - 1) ? lineitemFileSize : (i + 1) * lineitemChunkSize;
            double localTotalQuantity = 0.0;
            size_t localCount = 0;
            const char* lineitemEnd = lineitemFile.begin() + end;
            while (lineitemData < lineitemEnd) {
                const char* lineEnd = findNl(lineitemData, lineitemEnd);
                const char* orderkeyEnd = findBars(lineitemData, lineEnd, 1);
                const char* quantityStart = findBars(lineitemData, lineEnd, 4) + 1;
                const char* quantityEnd = findBars(quantityStart, lineEnd, 1);
                if (orderToCustkey.find(std::string(lineitemData, orderkeyEnd)) != orderToCustkey.end()) {
                    localTotalQuantity += std::stod(std::string(quantityStart, quantityEnd));
                    localCount++;
                }
                lineitemData = lineEnd + 1;
            }
            {
                std::lock_guard<std::mutex> lock(lineitemMutex);
                totalQuantity += localTotalQuantity;
                count += localCount;
            }
        });
    }

    for (auto& thread : lineitemThreads) {
        thread.join();
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