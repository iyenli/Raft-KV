#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <unordered_set>
#include <stdarg.h>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;

template<typename state_machine, typename command>
class raft {

    static_assert(std::is_base_of<raft_state_machine, state_machine>(),

    "state_machine must inherit from raft_state_machine");

    static_assert(std::is_base_of<raft_command, command>(),

    "command must inherit from raft_command");


    friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        auto now = \
        std::chrono::duration_cast<std::chrono::milliseconds>(\
            std::chrono::system_clock::now().time_since_epoch()\
        ).count();\
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while(0);

public:
    raft(
            rpcs *rpc_server,
            std::vector<rpcc *> rpc_clients,
            int idx,
            raft_storage<command> *storage,
            state_machine *state
    );

    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx;                     // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage;              // To persist the raft log
    state_machine *state;  // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;               // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                     // The index of this node in rpc_clients, start from 0
    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:
    int voted_for; // current term I vote for whom
    int commit_index;
    int last_applied;

    // reinitialized when elected
    std::vector<int> next_index;
    std::vector<int> match_index;
    std::vector<bool> syn_index;

    std::unordered_set<int> voter_for_self;

    // basic data
    std::vector<log_entry<command>> log;

    // Added: some time stamp recording
    std::chrono::milliseconds::rep last_rpc_time;
    std::chrono::milliseconds::rep last_ping_time;

    // snapshot part
    int last_included_index;
    std::vector<char> snapshot_data;

private:
    // Added: static threshold
    std::chrono::milliseconds ping_timeout;

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);

    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);

    void
    handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);

    void
    handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

    int logic2fact(const int &idx);

    int fact2logic(const int &idx);

private:
    bool is_stopped();

    int num_nodes() { return rpc_clients.size(); }

    // background workers
    void run_background_ping();

    void run_background_election();

    void run_background_commit();

    void run_background_apply();

    // Your code here:
    request_vote_args get_voter_args();

    void set_now(std::chrono::milliseconds::rep &timer);

    int add_to_log(command &command_);

    log_entry<command> get_log_entry(int index);

    void start_new_election();

};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage,
                                   state_machine *state) :
        storage(storage),
        state(state),
        rpc_server(server),
        rpc_clients(clients),
        my_id(idx),
        stopped(false),
        role(follower),
        current_term(0),
        background_election(nullptr),
        background_ping(nullptr),
        background_commit(nullptr),
        background_apply(nullptr) {
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    srand((int) (time(NULL)));
    // Your code here:
    // Do the initialization
    voted_for = -1;
    last_applied = 1; // Different from paper: NEXT should be applied
    commit_index = 0; // Same to paper, commit to where
    last_included_index = 0; // last snapshot idx
    ping_timeout = (std::chrono::milliseconds(150));

    // A huge change, from now on, start from 1 to n!!
    log_entry<command> init_cmd;
    init_cmd.term = -1;
    log.clear();
    log.push_back(init_cmd);

    storage->recovery(current_term, voted_for, log);
    storage->recover_snapshot(last_included_index, log, snapshot_data);
    if (last_included_index != 0) {
        // has sth to restore
        commit_index = last_included_index;
        last_applied = last_included_index + 1;
        while (!storage->update(current_term, voted_for, log)) {}
        ((raft_state_machine *) state)->apply_snapshot(snapshot_data);
//        RAFT_LOG("Recover from snap shot, term: %d, last index: %d", log[0].term, last_included_index);
    }

}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;
}

/********************************************************** || ********

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Your code here:

//    RAFT_LOG("start a new node, my id is %d", my_id);
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

/**
 *
 * @tparam state_machine
 * @tparam command
 * @param cmd Leader receive command and
 * @param term return true iff it's leader
 * @param index if true, fill in other 2 params
 * @return
 */
template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:
    mtx.lock();
    if (role != leader) {
        mtx.unlock();
        return false;
    }

    term = current_term;
    index = add_to_log(cmd);

    mtx.unlock();
    return true;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Your code here:
    mtx.lock();

    int snapshot_end_log = std::min(last_applied, commit_index);
    if (snapshot_end_log <= last_included_index) {
        // maybe recovered yet and wait for commit id and applied id recover
//        RAFT_LOG("Snap shot, ready install to %d, already install to %d",
//                 snapshot_end_log, last_included_index);
        goto success_return;
    }

    // install now!
    log[0].term = get_log_entry(snapshot_end_log).term;
    log.erase(log.begin() + 1, log.begin() + 1 + fact2logic(snapshot_end_log));
    snapshot_data = ((raft_state_machine *) state)->snapshot();

    last_included_index = snapshot_end_log;
    while (!storage->install_snapshot(last_included_index, log, snapshot_data)) {}
    while (!storage->update(current_term, voted_for, log)) {}
//    RAFT_LOG("Snap shot, install to %d, already install to %d, term: %d",
//             snapshot_end_log, last_included_index, log[0].term);

    success_return:
    mtx.unlock();
    return true;
}


/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Your code here:
    mtx.lock();

    reply.follower_term = current_term;

    if (current_term > args.current_term) {
        // candidates term id not the newest
        goto reject;
    } else if (current_term == args.current_term) {
        if (voted_for == -1 || voted_for == args.candidate_id) { // never vote for a candidate
            goto check_index;
        } else {
            goto reject; // only vote for first candidate
        }
    } else {
        // seen a larger term, convert to follower
        // and update term id
        // persist first
        role = follower;
        current_term = args.current_term;
        voted_for = -1;
        while (!storage->update(current_term, voted_for, log)) {}
//        RAFT_LOG("Term update to %d", current_term);
        goto check_index;
    }

    check_index:
    if (logic2fact(log.size()) == 1 || args.last_log_term > log.back().term ||
        (args.last_log_term == log.back().term &&
         args.last_log_index >= logic2fact(static_cast<int>(log.size() - 1)))) {
        reply.vote_granted = true;
        voted_for = args.candidate_id;
        while (!storage->update(current_term, voted_for, log)) {}
        set_now(last_rpc_time);
        mtx.unlock();
        return 0;
    } else {
        goto reject;
    }

    reject:
    reply.vote_granted = false;
    set_now(last_rpc_time);
    mtx.unlock();
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg,
                                                             const request_vote_reply &reply) {
    // Your code here:
    mtx.lock();
    if (reply.follower_term > current_term) {
        current_term = reply.follower_term;
        voted_for = -1;
        while (!storage->update(current_term, voted_for, log)) {}
//        RAFT_LOG("Term update to %d", current_term);
        role = follower;
    }
    if (role == candidate && reply.vote_granted) {
        voter_for_self.insert(target);
        size_t cluster_size = rpc_clients.size();
        if (voter_for_self.size() * 2 > cluster_size) {
            // I'm leader!!!
//            RAFT_LOG("Successful become leader: %d", static_cast<int>(voter_for_self.size()));
            role = leader;
//            auto current_time = duration_cast<std::chrono::milliseconds>(
//                    system_clock::now().time_since_epoch()).count();
//            last_ping_time = (current_time - ping_timeout.count());
            set_now(last_ping_time);
            int index_size = logic2fact(log.size());
            next_index.resize(cluster_size, index_size);
            match_index.resize(cluster_size, 0);
            syn_index.resize(cluster_size, false);
        }
    }
    set_now(last_rpc_time);
    mtx.unlock();
    return;
}


/**
 * follower or leader itself or candidate
 * receive REAL Request or heart beat, handler
 * @tparam state_machine
 * @tparam command
 * @param arg
 * @param reply
 * @return
 */
template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Your code here:
    mtx.lock();
    reply.reply_term = current_term;

    int append_start, last_index = logic2fact(log.size() - 1);
    int new_size = arg.entries.size();

    if (arg.leader_term > current_term) {
        current_term = arg.leader_term;
        voted_for = -1;
        role = follower;
        while (!storage->update(current_term, voted_for, log)) {}
    } // first update leader or mine
    else if (arg.leader_term < current_term) {
        goto fail_return;
    }

    // for leader, Append only!
    if (role == leader) {
        goto success_return;
    }

    if (arg.prev_log_index == 0 ||
        (last_index >= arg.prev_log_index &&
         get_log_entry(arg.prev_log_index).term == arg.prev_log_term)) {
        goto add2local;
    } else {
//        RAFT_LOG("Prev info not satisfied, prev log: index %d, term %d, current log size %d",
//                 arg.prev_log_index, arg.prev_log_term, last_index);
        goto fail_return;
    }

    fail_return:
    reply.success = false; // never match
    set_now(last_rpc_time);
    mtx.unlock();
    return 0;

    add2local:
    // if any conflict, cut them
    for (int i = 0; i < new_size; ++i) {
        int idx = arg.prev_log_index + 1 + i;
        if (last_index >= idx && idx >= last_included_index) { // buggy here!
            if (get_log_entry(idx).term != arg.entries[i].term) {
//                RAFT_LOG("TRUNCATE HAPPENS. Cut conflict, origin: %d, current: %d", last_index, idx);
                log.resize(fact2logic(idx));
                last_index = logic2fact(log.size() - 1);
                while (!storage->update(current_term, voted_for, log)) {}
                break;
            }
        } else {
            break;
        }
    }
    // if we have logs never heard, add them
    append_start = last_index - arg.prev_log_index;
    for (; append_start < new_size; ++append_start) {
        int idx = arg.prev_log_index + 1 + append_start;
        if (last_index >= idx) {
            assert(0);
        }
        log.push_back(arg.entries[append_start]);
        assert(((int) logic2fact(log.size()) == idx + 1));
    }
    while (!storage->update(current_term, voted_for, log)) {}
    // if leader commit id is larger:
    if (arg.leader_commit_index > commit_index) {
        commit_index = std::min(arg.leader_commit_index, logic2fact(static_cast<int>(log.size() - 1)));
    }

    success_return:
    reply.success = true;
    set_now(last_rpc_time);
    mtx.unlock();
    return 0;
}

/**
 * for leader, if receive append entries reply:
 * @tparam state_machine
 * @tparam command
 * @param target
 * @param arg
 * @param reply
 */

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command> &arg,
                                                               const append_entries_reply &reply) {
    // Your code here:
    mtx.lock();
    if (role == leader) { // In any case, we turn to follower when meeting larger term
        if (reply.reply_term > current_term) {
            current_term = reply.reply_term;
            voted_for = -1;
            while (!storage->update(current_term, voted_for, log)) {}
//            RAFT_LOG("LOSE POWER. Term update to %d", current_term);
            role = follower;
        } else { // only care about real entry appending
            if (reply.success) { // appending successfully
                // update next_index and match index
                int match_to = arg.prev_log_index + arg.entries.size();
                next_index[target] = match_to + 1;
                match_index[target] = match_to;
                syn_index[target] = true;

                // update commit id iff:
                // 1. commit id < match[target]
                // 2. log[match[target]].term == current term
                // 3. over majority accept this log
                // 4. really has sth to match
                if (match_to > commit_index) {
                    auto ent = get_log_entry(match_to);
                    if (ent.term == current_term) {
                        // check maj
                        int votes = 0, cluster_size = rpc_clients.size();
                        for (int i = 0; i < cluster_size; ++i) {
                            if (match_index[i] >= match_to) {
                                ++votes;
                            }
                        }
                        if (votes * 2 > cluster_size) {
//                            RAFT_LOG("New commit id, id: %d", match_to);
                            commit_index = match_to;
                        }
                    }
                }
            } else if (!syn_index[target]) {
                // only update next_index
                next_index[target]--;
                assert(next_index[target] >= 1);
            } else {
//                RAFT_LOG("Initialized but still fail，Please Pay attention");
                next_index[target]--;
            }
        }
    }

    set_now(last_rpc_time);
    mtx.unlock();
}

template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Your code here:
    mtx.lock();
    set_now(last_rpc_time);
    reply.reply_term = current_term;

    if (args.leader_term > current_term) {
        voted_for = -1;
        role = follower;
        current_term = args.leader_term;
        while (!storage->update(current_term, voted_for, log)) {}
    }
    if (args.leader_term < current_term || role == leader
        || args.last_included_index <= last_included_index) {
        goto direct_return;
    }
//    RAFT_LOG("Snapshot received! idx: %d", args.last_included_index);
    // follower receive snapshot never received
    snapshot_data = args.data;

    // truncate
    if (fact2logic(args.last_included_index) < (int) (log.size()) && log.size() > 1 &&
        get_log_entry(args.last_included_index).term == args.last_included_term) {
//        RAFT_LOG("Cut part of log, idx: %d", args.last_included_index);
        log.erase(log.begin() + 1, log.begin() + fact2logic(args.last_included_index) + 1);
        if (last_applied < args.last_included_index || commit_index < args.last_included_index) {
            RAFT_LOG("Weird! Why snapshot come first than commit id?");
            commit_index = args.last_included_index;
            last_applied = args.last_included_index + 1;
            ((raft_state_machine *) state)->apply_snapshot(args.data);
        }
        goto save_return;
    } else {
        // discard!
//        RAFT_LOG("Discard all to install");
        log.resize(1);
        commit_index = args.last_included_index;
        last_applied = args.last_included_index + 1;
        ((raft_state_machine *) state)->apply_snapshot(args.data);

        goto save_return;
    }

    // maybe buggy here
    save_return:
    last_included_index = args.last_included_index;
    log[0].term = args.last_included_term;
//    RAFT_LOG("Recover from snap shot, term: %d, last index: %d", log[0].term, last_included_index);
    while (!storage->install_snapshot(last_included_index, log, snapshot_data)) {}
    while (!storage->update(current_term, voted_for, log)) {}

    direct_return:
    mtx.unlock();
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args &arg,
                                                                 const install_snapshot_reply &reply) {
    // Your code here:
    mtx.lock();
    set_now(last_rpc_time);
    if (reply.reply_term > current_term) {
        voted_for = -1;
        role = follower;
        current_term = reply.reply_term;
        while (!storage->update(current_term, voted_for, log)) {}
    } else {
        // what to do?
        int next_idx = next_index[target], match_idx = match_index[target];
        next_index[target] = std::max(next_idx, arg.last_included_index + 1);
        match_index[target] = std::max(match_idx, arg.last_included_index);
        syn_index[target] = true;
    }
    mtx.unlock();
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Check the liveness of the leader.
    // Work for followers and candidates.

    // Hints: You should record the time you received the last RPC.
    //        And in this function, you can compare the current time with it.
    //        For example:
    //        if (current_time - last_received_RPC_time > timeout) start_election();
    //        Actually, the timeout should be different between the follower (e.g. 300-500ms) and the candidate (e.g. 1s).
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        auto current_time = duration_cast<std::chrono::milliseconds>(system_clock::now().time_since_epoch()).count();
        if (role == follower &&
            (current_time - last_rpc_time) > (std::chrono::milliseconds((rand() % 200) + 300)).count()) {
            // start an election
            mtx.lock();
            start_new_election();
            mtx.unlock();
        } else if (role == candidate &&
                   (current_time - last_rpc_time) > (std::chrono::milliseconds((rand() % 1000) + 1000)).count()) {
            // candidate timeout
            mtx.lock();
            start_new_election();
            mtx.unlock();
        }
//        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return;
}

// TODO: For leader, improve commitID here when detected majority accept the newest log
template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.

    while (true) {
        if (is_stopped()) return;
        // Your code here:
        if (role == leader) {
            mtx.lock();
            int log_size = logic2fact(log.size());
            int cluster_size = rpc_clients.size();

            append_entries_args<command> args;
            install_snapshot_args snapshot_args;
            args.leader_id = my_id;
            args.leader_term = current_term;
            args.leader_commit_index = commit_index;
            snapshot_args.leader_id = my_id;
            snapshot_args.leader_term = current_term;
            snapshot_args.last_included_index = last_included_index;
            snapshot_args.last_included_term = log[0].term;
            snapshot_args.offset = 0; // never used
            snapshot_args.done = true; // never used
            snapshot_args.data = snapshot_data;

            for (int i = 0; i < cluster_size; ++i) {
                int next_idx = next_index[i];

                if (syn_index[i] && next_idx >= log_size) {
                    continue;
                }

                if (last_included_index >= next_idx) {
                    thread_pool->addObjJob(this, &raft::send_install_snapshot, i, snapshot_args);

                } else {
                    assert(next_idx >= 1);
                    args.prev_log_index = next_idx - 1;
                    args.prev_log_term = get_log_entry(next_idx - 1).term; // never used

                    std::vector <log_entry<command>> commands;
                    for (int i = next_idx; i < log_size; ++i) {
                        auto tmp = get_log_entry(i);
                        commands.push_back(tmp);
                    }
                    args.entries = commands;
                    thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
                }
            }
            mtx.unlock();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    // Hints: You should check the commit index and the apply index.
    //        Update the apply index and apply the log if commit_index > apply_index

    while (true) {
        if (is_stopped()) return;
        // Your code here:
        mtx.lock();
        if (commit_index >= logic2fact(static_cast<int>(log.size()))) {
//            RAFT_LOG("Error: commit id = %d, log size = %d", commit_index, (int) log.size());
            assert(0);
        }
        for (; last_applied <= commit_index; ++last_applied) {
            auto ent = get_log_entry(last_applied); // BUGGY here
//            RAFT_LOG("Commit id = %d, applied id = %d", commit_index, last_applied);
            ((raft_state_machine *) state)->apply_log(ent.cmd);
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.
    // Only work for the leader.
    while (true) {
        if (is_stopped()) return;
        // Your code here:

        if (role == leader) {
            auto current_time = duration_cast<std::chrono::milliseconds>(
                    system_clock::now().time_since_epoch()).count();
            if ((current_time - last_ping_time) > ping_timeout.count()) {
                mtx.lock();
                set_now(last_ping_time);
                append_entries_args<command> args;
                args.leader_term = current_term;
                args.leader_id = my_id;
                args.leader_commit_index = commit_index;
                args.entries = std::vector<log_entry<command>>(0);

                int cluster_size = rpc_clients.size();
                for (int i = 0; i < cluster_size; ++i) {
                    int next_idx = next_index[i];
                    assert(next_idx >= 1);
                    if ((next_idx) <= last_included_index) {
                        continue;
                    } else {
                        args.prev_log_index = next_idx - 1;
                        args.prev_log_term = get_log_entry(next_idx - 1).term;
                    }
//                    RAFT_LOG("RPC Happens, Ping");
                    thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
                }


                mtx.unlock();
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); // Change the timeout here!
    }
    return;
}


/******************************************************************

                        Other functions

*******************************************************************/
/**
 *
 * @tparam state_machine
 * @tparam command
 * @return correspond request arg
 */
template<typename state_machine, typename command>
request_vote_args raft<state_machine, command>::get_voter_args() {
    request_vote_args args;
    args.current_term = current_term;
    args.candidate_id = my_id;

    args.last_log_index = logic2fact(static_cast<int>(log.size() - 1));
    args.last_log_term = log.back().term;

    return args;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::set_now(std::chrono::milliseconds::rep &timer) {
    timer = duration_cast<std::chrono::milliseconds>(
            system_clock::now().time_since_epoch()).count(); // prepare for election timeout
}

/**
 *
 * @tparam state_machine
 * @tparam command
 * @param command_
 * @return return REAL SIZE + 1!! corresponding to INDEX From 1 on
 */
template<typename state_machine, typename command>
int raft<state_machine, command>::add_to_log(command &command_) {
    log_entry<command> ent;
    ent.term = (current_term);
    ent.cmd = (command_);

    log.push_back(ent);
    while (!storage->update(current_term, voted_for, log)) {}
    return logic2fact(log.size() - 1);
}

/**
 *
 * @tparam state_machine
 * @tparam command
 * @param fact_index in our env, index means from 1 to size
 * @return log[index - 1]
 */
template<typename state_machine, typename command>
log_entry<command> raft<state_machine, command>::get_log_entry(int index) {
    int logic = fact2logic(index);

    if (!((logic >= 0 && logic < (int) log.size()))) {
//        RAFT_LOG("Error: get %d", logic);
        assert(0);
    }

    return log[logic];
}

/**
 * help start a new election, with much re-use code
 * @tparam state_machine
 * @tparam command
 */
template<typename state_machine, typename command>
void raft<state_machine, command>::start_new_election() {
    ++current_term;
    voted_for = my_id;
    role = candidate;
    voter_for_self.clear();
    voter_for_self.insert(my_id);
    while (!storage->update(current_term, voted_for, log)) {}
    // produce vote args and send out
    request_vote_args args = get_voter_args();
    int cluster_size = rpc_clients.size();
    for (int i = 0; i < cluster_size; ++i) {
//        RAFT_LOG("RPC Happens, ask for votes");
        thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
    }
}

template<typename state_machine, typename command>
int raft<state_machine, command>::logic2fact(const int &idx) {
    return idx + last_included_index; // log[0].term == last_included_term
}

template<typename state_machine, typename command>
int raft<state_machine, command>::fact2logic(const int &idx) {
    if (idx < last_included_index) {
//        RAFT_LOG("fact to logic, desired %d, last include %d", idx, last_included_index);
        assert(0);
    }

    return idx - last_included_index; // log[0].term == last_included_term
}

#endif // raft_h