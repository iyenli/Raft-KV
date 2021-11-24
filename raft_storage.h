#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include <vector>
#include <unistd.h>

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string &file_dir);

    ~raft_storage();

    // Your code here
    void recovery(int &current_term, int &vote_for, std::vector <log_entry<command>> &logs);

    bool append_log(const std::vector <log_entry<command>> &log);

    bool truncate_log(const int &idx);

    bool update_term(const int &term);

    bool update_vote(const int &vote_for);

private:
    std::mutex mtx;

    std::string meta_file_name;
    std::string log_file_name;
    std::string log_meta_file_name;

    bool need_recovery;
    int log_meta_size;

    std::vector <std::pair<int, int>> meta_log;

    std::string readAll(const std::fstream &);
};

template<typename command>
raft_storage<command>::raft_storage(const std::string &dir) {
    // Your code here
    mtx.lock();
    meta_file_name = dir + "/meta.rft";
    log_file_name = dir + "/log.rft";
    log_meta_file_name = dir + "/log_meta.rft";

    // iff need recovery, meta file must exist
    need_recovery = (access(meta_file_name.c_str(), F_OK) != -1);
    log_meta_size = static_cast<int>(sizeof(int) + sizeof(int));
    mtx.unlock();
}

template<typename command>
raft_storage<command>::~raft_storage() {
    // Your code here

}

template<typename command>
void raft_storage<command>::recovery(int &current_term, int &vote_for, std::vector <log_entry<command>> &logs) {
    if (!need_recovery) {
        return;
    }

    mtx.lock();
    // Open All persistent file
    std::fstream meta_file;
    std::fstream log_file;
    std::fstream log_meta_file;

    meta_file.open(meta_file_name, std::fstream::binary | std::fstream::in);
    log_file.open(log_file_name, std::fstream::in);
    log_meta_file.open(log_meta_file_name, std::fstream::binary | std::fstream::in);

    // move cursor to correct place
    log_file.seekg(0, std::ios::beg);
    meta_file.seekg(0, std::ios::beg);
    log_meta_file.seekg(0, std::ios::beg);

    meta_file >> vote_for >> current_term;

    int term, data_size;
    while (!log_meta_file.eof()) {
        log_meta_file >> term >> data_size;
        meta_log.push_back(std::make_pair(term, data_size));

        char *s = new char[data_size];
        log_file.read(s, data_size);

        // produce log entry and push back
        log_entry<command> tmp;
        tmp.term = term;
        ((raft_command) tmp.cmd).deserialize(s, data_size);
        logs.push_back(tmp);
    }

    log_meta_file.close();
    meta_file.close();
    log_file.close();
    mtx.unlock();
}

template<typename command>
std::string raft_storage<command>::readAll(const std::fstream &f) {
    std::istreambuf_iterator<char> begin(f);
    std::istreambuf_iterator<char> end;
    std::string ret(begin, end);
    return ret;
}

template<typename command>
bool raft_storage<command>::append_log(const std::vector <log_entry<command>> &log) {
    mtx.lock();
    std::fstream log_file;
    std::fstream log_meta_file;
    log_file.open(log_file_name, std::fstream::app | std::fstream::out);
    log_meta_file.open(log_meta_file_name, std::fstream::app | std::fstream::binary | std::fstream::out);

    int term, data_size, entries_num = log.size();
    for (int i = 0; i < entries_num; ++i) {
        term = log[i].term;
        auto cmd = log[i].cmd;
        data_size = ((raft_command)cmd).size();
        char *s = new char[data_size];

        ((raft_command)cmd).serialize(s, data_size);
        log_meta_file << term << data_size;
        log_file.write(s, data_size);

        meta_log.push_back(std::make_pair(term, data_size));
    }
    log_meta_file.close();
    log_file.close();

    mtx.unlock();
    return true;
}


template<typename command>
bool raft_storage<command>::update_vote(const int &vote_for) {
    std::ifstream ifs(meta_file_name, std::ifstream::binary);
    int current_term, vote_for_tmp;
    ifs >> vote_for_tmp >> current_term;
    ifs.close();

    // truncate and overwrite
    std::ofstream ofs(meta_file_name, std::ofstream::binary | std::ofstream::trunc);
    ofs << vote_for << current_term;
    ofs.close();
    return true;
}

template<typename command>
bool raft_storage<command>::update_term(const int &term) {
    std::ifstream ifs(meta_file_name, std::ifstream::binary);
    int current_term, vote_for;
    ifs >> vote_for >> current_term;
    ifs.close();

    // truncate and overwrite
    std::ofstream ofs(meta_file_name, std::ofstream::binary | std::ofstream::trunc);
    ofs << vote_for << term;
    ofs.close();
    return true;
}

/**
 *
 * @tparam command
 * @param idx I'll persist 1-n command in storage
 * Input the last index you want to keep, if is zero, cut to zero!
 * @return
 */
template<typename command>
bool raft_storage<command>::truncate_log(const int &idx) {
    // if input index of n, I'll truncate to 0 - (n - 1)
    int new_log_size = 0, new_meta_size = log_meta_size * idx;
    int n = std::min(idx, static_cast<int>(meta_log.size()));
    for(int i = 0; i < n; ++i){
        new_log_size += meta_log[i].second;
    }

    truncate(log_file_name.c_str(), new_log_size);
    truncate(log_meta_file_name.c_str(), new_meta_size);
    return true;
}


#endif // raft_storage_h