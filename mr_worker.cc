#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <map>
#include <algorithm>
#include <mutex>
#include <string>
#include <vector>
#include <map>

#include "rpc.h"
#include "mr_protocol.h"

using namespace std;

#define isAlphabet(c) ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))

struct KeyVal {
    string key;
    string val;
};

vector <KeyVal> Map(const string &filename, const string &content) {
    // Copy your code from mr_sequential.cc here.
    map<string, int> mp;
    vector <KeyVal> ret;
    int i = 0, j = 0, n = content.size();
    while (i < n) {
        while (i < n && !(isAlphabet(content[i])))
            ++i;

        j = i + 1;
        while (j < n && isAlphabet(content[j]))
            ++j;

        if (i < n) {
            ++mp[content.substr(i, (j - i))];
        }
        i = j + 1;
    }

    for (auto iter = mp.begin(); iter != mp.end(); ++iter) {
        KeyVal k;
        k.key = iter->first;
        k.val = to_string(iter->second);
        ret.push_back(k);
    }

    return ret;
}

string Reduce(const string &key, const vector <string> &values) {
    // Copy your code from mr_sequential.cc here.
    int sum = 0;
    for (auto &str: values) {
        sum += atoi(str.c_str());
    }
    return to_string(sum);
}


typedef vector<KeyVal> (*MAPF)(const string &key, const string &value);

typedef string (*REDUCEF)(const string &key, const vector <string> &values);

class Worker {
public:
    Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf);

    void doWork();

private:
    void doMap(int index, const string fileName);

    void doReduce(int index, int n);

    void doSubmit(mr_tasktype taskType, int index);

    mutex mtx;
    int id;

    rpcc *cl;
    std::string basedir;
    MAPF mapf;
    REDUCEF reducef;
};


Worker::Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf) {
    this->basedir = dir;
    this->mapf = mf;
    this->reducef = rf;

    sockaddr_in dstsock;
    make_sockaddr(dst.c_str(), &dstsock);
    this->cl = new rpcc(dstsock);
    if (this->cl->bind() < 0) {
        printf("mr worker: call bind error\n");
    }
}

void Worker::doMap(int index, const string fileName) {
    // Lab2: Your code goes here.
    // read file first
    string filePath = basedir + fileName;
    ifstream ifs(filePath);
    string content((istreambuf_iterator<char>(ifs)), (istreambuf_iterator<char>()));
    ifs.close();

    vector <KeyVal> kvs = Map(fileName, content);
    vector <vector<KeyVal>> kv2write(REDUCER_COUNT, vector<KeyVal>(0));

    for (auto &kv: kvs) {
        int sum = 0;
        for (auto &c: kv.key) {
            sum += c - 'A';
        }
        sum %= 4;
        kv2write[sum].push_back(kv);
    }

    for (int i = 0; i < REDUCER_COUNT; ++i) {
        string writeName = basedir + "mr-";
        writeName += to_string(index);
        writeName += "-";
        writeName += to_string(i);

        string buf = "";
        for (auto &kv: kv2write[i]) {
            buf += kv.key;
            buf += ":";
            buf += kv.val;
            buf += ";";
        }

        ofstream fileSink(writeName);
        fileSink.write(buf.c_str(), buf.size());
        fileSink.close();
    }
}

vector <string> splitString(string s, char c) {
    size_t pos = 0;
    string token;
    vector <string> ret;

    while ((pos = s.find(c)) != std::string::npos) {
        token = s.substr(0, pos);
        if (token != "")
            ret.push_back(token);
        s.erase(0, pos + 1);
    }
    if (s != "")
        ret.push_back(s);

    return ret;
}

void Worker::doReduce(int index, int n) {
    // Lab2: Your code goes here.
    vector <KeyVal> intermediate;
    for (int i = 0; i < n; ++i) {
        string fileName = basedir + "mr-" + to_string(i) + "-" + to_string(index);
        ifstream f(fileName);
        if (!f.good()) {
            f.close();
            continue;
        }
        string content((istreambuf_iterator<char>(f)), (istreambuf_iterator<char>()));
        f.close();

        vector <string> kvString = splitString(content, ';');

        for (auto &kv: kvString) {
            vector <string> keyValue = splitString(kv, ':');
            KeyVal p;
            p.key = keyValue[0];
            p.val = keyValue[1];
            intermediate.push_back(p);
        }
    }

    sort(intermediate.begin(), intermediate.end(),
         [](KeyVal const &a, KeyVal const &b) {
             return a.key < b.key;
         });

    string buffer = "";
    unsigned int sizeOfIntermediate = intermediate.size();
    for (unsigned int i = 0; i < sizeOfIntermediate;) {
        unsigned int j = i + 1;
        for (; j < sizeOfIntermediate && intermediate[j].key == intermediate[i].key;)
            j++;

        vector <string> values;
        for (unsigned int k = i; k < j; k++) {
            values.push_back(intermediate[k].val);
        }

        string output = Reduce(intermediate[i].key, values);
        buffer += (intermediate[i].key + " " + output + "\n");
        i = j;
    }

    string outputFile = "mr-out-" + to_string(index);
    ofstream fileSink(outputFile);
    fileSink.write(buffer.c_str(), buffer.size());
    fileSink.close();
}

void Worker::doSubmit(mr_tasktype taskType, int index) {
    bool b;
    mr_protocol::status ret = this->cl->call(mr_protocol::submittask, taskType, index, b);
    if (ret != mr_protocol::OK) {
        fprintf(stderr, "submit task failed\n");
        exit(-1);
    }
}

void Worker::doWork() {
    for (;;) {
        // Lab2: Your code goes here.
        // Hints: send asktask RPC call to coordinator
        // if mr_tasktype::MAP, then doMap and doSubmit
        // if mr_tasktype::REDUCE, then doReduce and doSubmit
        // if mr_tasktype::NONE, meaning currently no work is needed, then sleep
        int empty;
        mr_protocol::AskTaskResponse task;
        mr_protocol::status ret = this->cl->call(mr_protocol::asktask, empty, task);
        if (ret != mr_protocol::OK) {
            fprintf(stderr, "submit task failed\n");
            exit(-1);
        }

        if (task.type == NONE) {
            sleep(1);
        } else if (task.type == MAP) {
            string fileName = task.fileName;
            doMap(task.index, fileName);
            doSubmit(MAP, task.index);
        } else if (task.type == REDUCE) {
            doReduce(task.index, atoi(task.fileName.c_str()));
            doSubmit(REDUCE, task.index);
        } else {
            exit(-1);
        }
    }
}

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <coordinator_listen_port> <intermediate_file_dir> \n", argv[0]);
        exit(1);
    }

    MAPF mf = Map;
    REDUCEF rf = Reduce;

    Worker w(argv[1], argv[2], mf, rf);
    w.doWork();

    return 0;
}

