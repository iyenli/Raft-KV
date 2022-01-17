#include "ch_db.h"

int view_server::execute(unsigned int query_key, unsigned int proc, const chdb_protocol::operation_var &var, int &r) {
    // TODO: Your code here
    auto &rafts = raft_group->nodes;
    chdb_command command;
    command.tx_id = var.tx_id;
    command.key = var.key;
    command.value = var.value;
    command.cmd_tp = proc == chdb_protocol::Get ?
                     chdb_command::CMD_GET : chdb_command::CMD_PUT;

    int temp_idx, temp_term;
    auto leader = this->leader();
    leader->new_command(command, temp_term, temp_idx);

    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());

    return this->node->template call(base_port + shard_offset, proc, var, r);
}

/*******************************************************************
 *
 * send rpcs to all nodes
 *
 ******************************************************************/
int view_server::execute_commit(const chdb_protocol::commit_var &var,
                                int &r) {
    int base_port = this->node->port(), res = 0;
    auto shard_num = this->shard_num();

    for (auto i = 1; i <= shard_num; ++i) {
        res = this->node->template call(base_port + i, chdb_protocol::Commit, var, r);
    }
    return res;
}

int view_server::execute_prepare(const chdb_protocol::prepare_var &var,
                                 int &r) {
    int base_port = this->node->port(), res = 0;
    auto shard_num = this->shard_num();

    for (auto i = 1; i <= shard_num; ++i) {
        res = this->node->template call(base_port + i, chdb_protocol::Prepare, var, r);

    }
    return res;
}

int view_server::execute_check(const chdb_protocol::check_prepare_state_var &var,
                               int &r) {
    int base_port = this->node->port(), res = 0;
    auto shard_num = this->shard_num();

    for (auto i = 1; i <= shard_num; ++i) {
        res = this->node->template call(base_port + i, chdb_protocol::CheckPrepareState, var, r);
        if (r != chdb_protocol::prepare_ok) {
            return res;
        }
    }
    return res;
}

int view_server::execute_abort(const chdb_protocol::rollback_var &var,
                               int &r) {
    int base_port = this->node->port(), res = 0;
    auto shard_num = this->shard_num();
    for (auto i = 1; i <= shard_num; ++i) {
        res = this->node->template call(base_port + i, chdb_protocol::Rollback, var, r);
    }
    return res;
}


view_server::~view_server() {
#if RAFT_GROUP
    delete this->raft_group;
#endif
    delete this->node;

}