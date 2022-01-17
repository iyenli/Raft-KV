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

    void recover_snapshot(int &last_included_index, std::vector <log_entry<command>> &logs,
                          std::vector<char> &snapshot_data);

    bool install_snapshot(const int &last_included_index, const std::vector <log_entry<command>> &logs,
                          const std::vector<char> &snapshot_data);

//    bool append_log(const int &idx, const log_entry<command> &log);

    bool update(const int &term, const int &vote_for, const std::vector <log_entry<command>> &log);

//    bool truncate_log(const int &idx);

    bool update_term(const int &term);

    bool update_vote(const int &vote_for);

    bool update_meta(const int &vote_for, const int &term);

//    void truncate_file_directly(int idx);

private:
    std::mutex mtx;

    std::string meta_file_name;
    std::string log_file_name;
    std::string log_meta_file_name;
    std::string snapshot_file_name;
    std::string snapshot_meta_file_name;

    bool need_recovery, need_recover_snapshot;
    int log_meta_size;

    std::vector <std::pair<int, int>> meta_log;

    void write_int(std::fstream &, const int &);

    void read_int(std::fstream &, int &);
};

template<typename command>
raft_storage<command>::raft_storage(const std::string &dir) {
    // Your code here
    mtx.lock();
    meta_file_name = dir + "/meta.rft";
    log_file_name = dir + "/log.rft";
    log_meta_file_name = dir + "/log_meta.rft";
    snapshot_file_name = dir + "/snapshot.rft";
    snapshot_meta_file_name = dir + "/snapshot_meta.rft";

    // iff need recovery, meta file must exist
    need_recovery = (access(meta_file_name.c_str(), F_OK) != -1);
    need_recover_snapshot = (access(snapshot_meta_file_name.c_str(), F_OK) != -1);
    log_meta_size = static_cast<int>(sizeof(int) + sizeof(int));

    // init meta
    if (!need_recovery) {
        std::fstream meta_file;
        meta_file.open(meta_file_name, std::fstream::binary | std::fstream::out);
        int vote_for_init = -1, term_init = 0;
        write_int(meta_file, vote_for_init);
        write_int(meta_file, term_init);
    }
    mtx.unlock();
}

template<typename command>
raft_storage<command>::~raft_storage() {
    // Your code here

}

template<typename command>
void raft_storage<command>::recover_snapshot(int &last_included_index, std::vector <log_entry<command>> &logs,
                                             std::vector<char> &snapshot_data) {
    if (!need_recover_snapshot) {
        return;
    }
    mtx.lock();

    std::ifstream snapshot_file(snapshot_file_name, std::ifstream::binary);
    std::fstream snapshot_meta_file(snapshot_meta_file_name, std::fstream::binary | std::fstream::in);

    assert(logs.size() >= 1);
    int last_snapshot_index, last_snapshot_term;
    read_int(snapshot_meta_file, last_snapshot_index);
    read_int(snapshot_meta_file, last_snapshot_term);

    last_included_index = last_snapshot_index;
    logs[0].term = last_snapshot_term;

    std::istreambuf_iterator<char> begin(snapshot_file);
    std::istreambuf_iterator<char> end;
    std::string snapshot_data_in_file(begin, end);
    snapshot_data = std::vector<char>(snapshot_data_in_file.begin(), snapshot_data_in_file.end());

    snapshot_file.close();
    snapshot_meta_file.close();
    mtx.unlock();
}

template<typename command>
bool raft_storage<command>::install_snapshot(const int &last_included_index,
                                             const std::vector <log_entry<command>> &logs,
                                             const std::vector<char> &snapshot_data) {
    mtx.lock();
    std::fstream snapshot_file(snapshot_file_name, std::fstream::binary | std::fstream::trunc | std::fstream::out);
    std::fstream snapshot_meta_file(snapshot_meta_file_name,
                                    std::fstream::binary | std::fstream::trunc | std::fstream::out);

    assert(logs.size() >= 1);
    int last_snapshot_index = last_included_index, last_snapshot_term = logs[0].term;
    write_int(snapshot_meta_file, last_snapshot_index);
    write_int(snapshot_meta_file, last_snapshot_term);

    std::string input(snapshot_data.begin(), snapshot_data.end());
    snapshot_file.write(input.c_str(), input.size());

    snapshot_file.close();
    snapshot_meta_file.close();
    mtx.unlock();
    return true;
}

template<typename command>
void raft_storage<command>::recovery(int &current_term, int &vote_for,
                                     std::vector <log_entry<command>> &logs) {
    if (!need_recovery) {
        return;
    }
    mtx.lock();
    if (logs.size() != 1) { // keep bid
        printf("Error, A not-qualified logs vector input, size: %d", (int) logs.size());
        log_entry<command> ent;

        ent.term = -1;
        logs.clear();
        logs.push_back(ent);
    }

    // Open All persistent file
    std::fstream meta_file;
    std::fstream log_file;
    std::fstream log_meta_file;

    meta_file.open(meta_file_name, std::fstream::binary | std::fstream::in | std::fstream::out);
    log_file.open(log_file_name, std::fstream::binary | std::fstream::in);
    log_meta_file.open(log_meta_file_name, std::fstream::binary | std::fstream::in);

    // move cursor to correct place
    log_file.seekg(0, std::fstream::beg);
    meta_file.seekg(0, std::fstream::beg);
    log_meta_file.seekg(0, std::fstream::beg);

    read_int(meta_file, vote_for);
    read_int(meta_file, current_term);

    int term, data_size;
    meta_log.clear(); // keep bid
    while (!log_meta_file.eof()) {
        read_int(log_meta_file, term);
        read_int(log_meta_file, data_size);
        meta_log.push_back(std::make_pair(term, data_size));
//        printf("Read log here, data size = %d \n", static_cast<int>(data_size));

        char *s = new char[data_size];
        log_file.read(s, data_size);
//        if ((int) strlen(s) < data_size) {
//            break; // special case
//        }
        // produce log entry and push back
        log_entry<command> tmp;
        tmp.term = term;
        ((raft_command *) (&tmp.cmd))->deserialize(s, data_size);
        logs.push_back(tmp);
    }

    log_meta_file.close();
    meta_file.close();
    log_file.close();
    mtx.unlock();
}

template<typename command>
bool raft_storage<command>::update(const int &term, const int &vote_for, const std::vector <log_entry<command>> &log) {
    mtx.lock();
    std::fstream meta_file(meta_file_name, std::fstream::binary | std::fstream::trunc | std::fstream::out);
    std::fstream log_file(log_file_name, std::fstream::binary | std::fstream::trunc | std::fstream::out);
    std::fstream log_meta_file(log_meta_file_name, std::fstream::binary | std::fstream::trunc | std::fstream::out);

    write_int(meta_file, vote_for);
    write_int(meta_file, term);

    meta_log.clear();
    int log_term, data_size, n = log.size();
    assert(n >= 1);
    for (int i = 1; i < n; ++i) {
        log_term = log[i].term;
        data_size = ((raft_command *) (&(log[i].cmd)))->size();
        char *s = new char[data_size];
        ((raft_command *) (&(log[i].cmd)))->serialize(s, data_size);

        write_int(log_meta_file, log_term);
        write_int(log_meta_file, data_size);

        log_file.write(s, data_size);
        meta_log.push_back(std::make_pair(log_term, data_size));
    }

    log_meta_file.close();
    meta_file.close();
    log_file.close();
    mtx.unlock();
    return true;
}

template<typename command>
void raft_storage<command>::read_int(std::fstream &f, int &a) {
    int tmp;
    f.read((char *) (&tmp), sizeof(int));
    a = tmp;
}

template<typename command>
void raft_storage<command>::write_int(std::fstream &f, const int &a) {
    f.write((char *) (&a), sizeof(int));
}


// Disable efficient but buggy code!
/**
 *
 * @tparam command
 * @param idx I'll persist 1-n command in storage
 * Input the last index you want to keep, if is zero, cut to zero!
 * @return
 */
//template<typename command>
//bool raft_storage<command>::truncate_log(const int &idx) {
//    // if input index of n, I'll truncate to 0 - (n - 1)
//    int new_log_size = 0, new_meta_size = log_meta_size * idx;
//    int n = std::min(idx, static_cast<int>(meta_log.size()));
//    for (int i = 0; i < n; ++i) {
//        new_log_size += meta_log[i].second;
//    }
//
//    printf("Execute Truncate, meta size = %d, size = %d, log size = %d\n",
//           new_meta_size, idx, new_log_size);
//
//    truncate(log_file_name.c_str(), new_log_size);
//    truncate(log_meta_file_name.c_str(), new_meta_size);
//    if (idx > static_cast<int>(meta_log.size())) {
//        meta_log.resize(idx);
//    }
//
//    return true;
//}

//template<typename command>
//bool raft_storage<command>::append_log(const int &idx, const log_entry<command> &log) {
//    mtx.lock(); // keep bio!
//    if ((int) meta_log.size() > idx && meta_log[idx].first == log.term) {
//        return true; // done
//    } else if ((int) meta_log.size() > idx) {
//        meta_log.resize(idx);
//    }
//    std::fstream log_file;
//    std::fstream log_meta_file;
//
//    log_file.open(log_file_name, std::fstream::ate | std::fstream::out | std::fstream::binary);
//    log_meta_file.open(log_meta_file_name, std::fstream::ate | std::fstream::binary | std::fstream::out);
//
//    int term, data_size;
//    term = log.term;
//    auto cmd = log.cmd;
//    data_size = ((raft_command *) (&cmd))->size();
//    char *s = new char[data_size];
//
//    ((raft_command *) (&cmd))->serialize(s, data_size);
//    write_int(log_meta_file, term);
//    write_int(log_meta_file, data_size);
//
////    printf("Write log here, data size = %d \n", static_cast<int>(data_size));
//    log_file.write(s, data_size);
//
//    meta_log.push_back(std::make_pair(term, data_size));
//
//    log_meta_file.close();
//    log_file.close();
//
//    mtx.unlock();
//    return true;
//}


template<typename command>
bool raft_storage<command>::update_vote(const int &vote_for) {
    std::fstream meta_file(meta_file_name, std::fstream::binary | std::fstream::in);
    int current_term, vote_for_tmp;
    read_int(meta_file, vote_for_tmp);
    read_int(meta_file, current_term);
    meta_file.close();

    // truncate and overwrite
    meta_file.open(meta_file_name, std::fstream::binary | std::fstream::trunc | std::fstream::out);
    write_int(meta_file, vote_for);
    write_int(meta_file, current_term);
    meta_file.close();
    return true;
}

template<typename command>
bool raft_storage<command>::update_term(const int &term) {
    std::fstream meta_file(meta_file_name, std::fstream::binary | std::fstream::in);
    int current_term, vote_for_tmp;
    read_int(meta_file, vote_for_tmp);
    read_int(meta_file, current_term);
    meta_file.close();

    // truncate and overwrite
    meta_file.open(meta_file_name, std::fstream::binary | std::fstream::trunc | std::fstream::out);
    write_int(meta_file, vote_for_tmp);
    write_int(meta_file, term);
    meta_file.close();
    return true;
}

template<typename command>
bool raft_storage<command>::update_meta(const int &vote_for, const int &term) {
    std::fstream meta_file(meta_file_name, std::fstream::binary | std::fstream::trunc | std::fstream::out);
    write_int(meta_file, vote_for);
    write_int(meta_file, term);
    meta_file.close();
    return true;
}
//
//template<typename command>
//void raft_storage<command>::truncate_file_directly(int idx) {
//    // a more inefficient version
//    std::fstream log_file;
//    std::fstream log_meta_file;
//
//    log_file.open(log_file_name, std::fstream::in | std::fstream::binary);
//    log_meta_file.open(log_meta_file_name, std::fstream::binary | std::fstream::in);
//
//    // move cursor to correct place
//    log_file.seekg(0, std::fstream::beg);
//    log_meta_file.seekg(0, std::fstream::beg);
//
//    int term, data_size;
//    std::vector <log_entry<command>> logs;
//
//    meta_log.clear();
//    while (!log_meta_file.eof()) {
//        read_int(log_meta_file, term);
//        read_int(log_meta_file, data_size);
//        meta_log.push_back(std::make_pair(term, data_size));
//
//        char *s = new char[data_size];
//        log_file.read(s, data_size);
//
//        // produce log entry and push back
//        log_entry<command> tmp;
//        tmp.term = term;
//        ((raft_command *) (&tmp.cmd))->deserialize(s, data_size);
//        logs.push_back(tmp);
//    }
//
//    log_meta_file.close();
//    log_file.close();
//
//    int n = std::min(idx, (int) logs.size());
//    log_file.open(log_file_name, std::fstream::out | std::fstream::binary | std::fstream::trunc);
//    log_meta_file.open(log_meta_file_name, std::fstream::binary | std::fstream::out | std::fstream::trunc);
//
//    for (int i = 0; i < n; ++i) {
//        write_int(log_meta_file, meta_log[i].first);
//        write_int(log_meta_file, meta_log[i].second);
//
//        auto cmd = logs[i].cmd;
//        data_size = ((raft_command *) (&cmd))->size();
//        char *s = new char[data_size];
//        ((raft_command *) (&cmd))->serialize(s, data_size);
//        log_file.write(s, data_size);
//    }
//    log_meta_file.close();
//    log_file.close();
//    // inefficient method stops here
//}


#endif // raft_storage_h