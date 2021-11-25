#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
    OK,
    RETRY,
    RPCERR,
    NOENT,
    IOERR
};

class request_vote_args {
public:
    // Your code here
    int current_term;
    int candidate_id;
    int last_log_index;
    int last_log_term;
};

marshall &operator<<(marshall &m, const request_vote_args &args);

unmarshall &operator>>(unmarshall &u, request_vote_args &args);


class request_vote_reply {
public:
    // Your code here
    int follower_term;
    bool vote_granted;
};

marshall &operator<<(marshall &m, const request_vote_reply &reply);

unmarshall &operator>>(unmarshall &u, request_vote_reply &reply);

template<typename command>
class log_entry {
public:
    // Your code here
    int term;
    command cmd;
};

template<typename command>
marshall &operator<<(marshall &m, const log_entry<command> &entry) {
    // Your code here
    m << entry.cmd << entry.term;
    return m;
}

template<typename command>
unmarshall &operator>>(unmarshall &u, log_entry<command> &entry) {
    // Your code here
    u >> entry.cmd >> entry.term;
    return u;
}

template<typename command>
class append_entries_args {
public:
    // Your code here
    int leader_term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;

    std::vector <log_entry<command>> entries;
    int leader_commit_index;

};

template<typename command>
marshall &operator<<(marshall &m, const append_entries_args<command> &args) {
    // Your code here
    m << args.leader_id << args.leader_term <<
      args.prev_log_index << args.prev_log_term << args.leader_commit_index;
    int size_ = args.entries.size();
    m << size_;

    for (int i = 0; i < size_; ++i) {
        m << args.entries[i].term << args.entries[i].cmd;
    }
    return m;
}

template<typename command>
unmarshall &operator>>(unmarshall &u, append_entries_args<command> &args) {
    // Your code here
    u >> args.leader_id >> args.leader_term >>
      args.prev_log_index >> args.prev_log_term >> args.leader_commit_index;

    int size_;
    u >> size_;
    for (int i = 0; i < size_; ++i) {
        log_entry<command> t;
        u >> t.term >> t.cmd;
        args.entries.push_back(t);
    }

    return u;
}

class append_entries_reply {
public:
    // Your code here
    int reply_term;
    bool success;
};

marshall &operator<<(marshall &m, const append_entries_reply &reply);

unmarshall &operator>>(unmarshall &m, append_entries_reply &reply);


class install_snapshot_args {
public:
    // Your code here
    int leader_term;
    int leader_id;
    int last_included_index;
    int last_included_term;
    int offset;

    std::vector<char> data;
    bool done;
};

marshall &operator<<(marshall &m, const install_snapshot_args &args);

unmarshall &operator>>(unmarshall &m, install_snapshot_args &args);


class install_snapshot_reply {
public:
    // Your code here
    int reply_term;
};

marshall &operator<<(marshall &m, const install_snapshot_reply &reply);

unmarshall &operator>>(unmarshall &m, install_snapshot_reply &reply);


#endif // raft_protocol_h