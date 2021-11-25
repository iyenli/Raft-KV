#include "raft_protocol.h"

marshall &operator<<(marshall &m, const request_vote_args &args) {
    // Your code here
    m << args.candidate_id << args.current_term << args.last_log_index << args.last_log_term;
    return m;

}

unmarshall &operator>>(unmarshall &u, request_vote_args &args) {
    // Your code here
    u >> args.candidate_id >> args.current_term >> args.last_log_index >> args.last_log_term;
    return u;
}

marshall &operator<<(marshall &m, const request_vote_reply &reply) {
    // Your code here
    m << reply.follower_term << reply.vote_granted;
    return m;
}

unmarshall &operator>>(unmarshall &u, request_vote_reply &reply) {
    // Your code here
    u >> reply.follower_term >> reply.vote_granted;
    return u;
}

marshall &operator<<(marshall &m, const append_entries_reply &reply) {
    // Your code here
    m << reply.reply_term << reply.success;
    return m;
}

unmarshall &operator>>(unmarshall &m, append_entries_reply &reply) {
    // Your code here
    m >> reply.reply_term >> reply.success;
    return m;
}

marshall &operator<<(marshall &m, const install_snapshot_args &args) {
    // Your code here
    
    return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_args &args) {
    // Your code here

    return u;
}

marshall &operator<<(marshall &m, const install_snapshot_reply &reply) {
    // Your code here

    return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_reply &reply) {
    // Your code here

    return u;
}