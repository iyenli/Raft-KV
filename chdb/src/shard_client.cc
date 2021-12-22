#include "shard_client.h"


int shard_client::put(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    auto &ops = wal[var.tx_id];
    for(auto &op: ops) {
        if(op.key == var.key) {
            op.value = var.value;
            return 0;
        }
    }
    ops.push_back(var);
    return 0;
}

int shard_client::get(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    auto &primary = this->get_store();
    auto &logs = wal[var.tx_id];
    for (auto &log: logs) {
        if (log.key == var.key) {
            r = log.value;
            return 0;
        }
    }

    if (primary.count(var.key)) {
        r = primary[var.key].value;
    } else {
        r = -1;
    }
    return 0;
}

int shard_client::commit(chdb_protocol::commit_var var, int &r) {
    // TODO: Your code here
    auto &vec = wal[var.tx_id];
//    auto &primary = this->get_store();
//    auto all = this->replica_num;
    for(auto &primary: this->store) {
        for (auto &op: vec) {
            auto val = value_entry();
            val.value = op.value;
            if (primary.count(op.key)) {
                primary[op.key] = val;
            } else {
                primary.insert({op.key, val});
            }
        }
    }


    wal.erase(var.tx_id);

    return 0;
}

int shard_client::rollback(chdb_protocol::rollback_var var, int &r) {
    // TODO: Your code here
    wal.erase(var.tx_id);
    return 0;
}

int shard_client::check_prepare_state(chdb_protocol::check_prepare_state_var var, int &r) {
    // TODO: Your code here
    r = chdb_protocol::prepare_ok;
    if (!wal[var.tx_id].empty() && !active) {
        r = chdb_protocol::prepare_not_ok;
    }
    return 0;
}

int shard_client::prepare(chdb_protocol::prepare_var var, int &r) {
    // TODO: Your code here
    wal.insert(std::pair<int, std::vector<chdb_protocol::operation_var>>(var.tx_id, std::vector<chdb_protocol::operation_var>()));
    return 0;
}

shard_client::~shard_client() {
    delete node;
}