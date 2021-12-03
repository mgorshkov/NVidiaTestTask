//
//  main.cpp
//  NVidiaTestTask
//
//  Created by Mikhail Gorshkov on 02.12.2021.
//

#include <iomanip>
#include <iostream>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <utility>
#include <sstream>
#include <fstream>
#include <assert.h>
/*

 Input:

    ```yaml
    #job_id,job_id_next,runtime_secs
    1,23,60
    2,3,23
    3,0,12
    23,0,30
    ```

Output pipeline statistics:

- Output:

    ```yaml
    ---
    start_job: 1
    last_job: 23
    job_len: 2
    total_runtime: 00:01:30
    avg_runtime: 00:00:45

    ---
    start_job: 2
    last_job: 3
    job_len: 2
    total_runtime: 00:00:35
    avg_runtime: 00:00:17
*/

using JobId = long;
struct Info {
    JobId end;
    int len;
    int totalRuntime;
    int avgRuntime;
};
std::map<JobId, Info> infos;
std::unordered_map<JobId, std::pair<JobId, int>> forward_chains;
std::unordered_map<JobId, std::pair<JobId, int>> backward_chains;
std::unordered_set<JobId> seen;

void processChainsFromStart(JobId start_node) {
    if (seen.find(start_node) != seen.end())
        return; // already seen this chain
    seen.insert(start_node);

    int sum = 0;
    int count = 0;
    auto node = start_node;
    while (true) {
        auto it = forward_chains.find(node);
        if (it == forward_chains.end())
            break;
        sum += it->second.second;
        ++count;
        if (it->second.first == 0)
            break; // end of chain
        node = it->second.first;
        seen.insert(node);
    }
    if (count == 0)
        return;
    Info info;
    info.totalRuntime = sum;
    info.avgRuntime = sum / count;
    info.end = node;
    info.len = count;
    infos[start_node] = info;
}

int fillForwardBackward(std::istream& input_stream) {
    std::string line;
    while (std::getline(input_stream, line)) {
        if (!line.empty() && line[0] == '#') // comment
            continue;
        const char* ptr = line.c_str();
        char* cur;
        errno = 0;
        long start = strtol(ptr, &cur, 10);
        if (errno != 0) {
            std::cerr << "Error " << errno;
            return 1;
        }
        if (*cur++ != ',') {
            std::cerr << "Error!";
            return 1;
        }
        errno = 0;
        ptr = cur;
        long next = strtol(ptr, &cur, 10);
        if (errno != 0) {
            std::cerr << "Error " << errno;
            return 1;
        }
        if (*cur++ != ',') {
            std::cerr << "Error!";
            return 1;
        }
        errno = 0;
        ptr = cur;
        long time = strtol(ptr, &cur, 10);
        
        forward_chains[start] = std::make_pair(next, time);
        backward_chains[next] = std::make_pair(start, time);
    }
    return 0;
}

void processChains() {
    for (auto v : forward_chains) {
        auto node = v.first;
        if (seen.find(node) != seen.end())
            continue; // already seen this chain
        auto it = backward_chains.find(node);
        while (it != backward_chains.end()) {
            node = it->second.first;
            it = backward_chains.find(node);
        }
        processChainsFromStart(node);
    }
}

std::ostream& outputTime(std::ostream& output_stream, int time) {
    output_stream << std::setfill('0') << std::setw(2) << int(time / 3600) << ":" << std::setfill('0') << std::setw(2) << int(time / 60) << ":" << std::setfill('0') << std::setw(2) << time % 60;
    return output_stream;
}

void outputChains(std::ostream& output_stream) {
    for (auto v : infos) {
        output_stream << "---" << std::endl;
        output_stream << "start_job: " << v.first << std::endl;
        output_stream << "last_job: " << v.second.end << std::endl;
        output_stream << "job_len: " << v.second.len << std::endl;
        output_stream << "total_runtime: ";
        outputTime(output_stream, v.second.totalRuntime);
        output_stream << std::endl;
        output_stream << "avg_runtime: ";
        outputTime(output_stream, v.second.avgRuntime);
        output_stream << std::endl << std::endl;
    }
}

int outputYamlJobStatistics(std::istream& input_stream, std::ostream& output_stream) {
    auto res = fillForwardBackward(input_stream);
    if (res != 0)
        return res;
    
    processChains();
    
    outputChains(output_stream);

    return 0;
}

int main() {
    const char* input = R"(#job_id,job_id_next,runtime_secs
1,23,60
2,3,23
3,0,12
23,0,30)";

    const char* output = R"(---
start_job: 1
last_job: 23
job_len: 2
total_runtime: 00:01:30
avg_runtime: 00:00:45

---
start_job: 2
last_job: 3
job_len: 2
total_runtime: 00:00:35
avg_runtime: 00:00:17

)";
    std::stringstream input_stream(input);
    std::stringstream output_stream;
    auto res = outputYamlJobStatistics(input_stream, output_stream);
    assert(res == 0);
    auto str = output_stream.str();
    assert(output_stream.str() == output);

    return 0;
}



