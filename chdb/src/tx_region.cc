#include "tx_region.h"


int tx_region::put(const int key, const int val) {
    // TODO: Your code here
    chdb_protocol::operation_var var;
    int res;
    var.tx_id = this->tx_id;
    var.key = key;
    var.value = val;

    this->db->vserver->execute(key, chdb_protocol::Put, var, res);
    return 0;
}

int tx_region::get(const int key) {
    // TODO: Your code here
    chdb_protocol::operation_var var;
    int res;
    var.tx_id = this->tx_id;
    var.key = key;

    this->db->vserver->execute(key, chdb_protocol::Get, var, res);
    return res;
}

int tx_region::tx_can_commit() {
    // TODO: Your code here
    chdb_protocol::check_prepare_state_var var;
    var.tx_id = this->tx_id;

    int res;
    this->db->vserver->execute_check(var, res);
    return res;
}

int tx_region::tx_begin() {
    // TODO: Your code here
    printf("tx[%d] begin\n", tx_id);
    this->db->vserver->mtx.lock();
    chdb_protocol::prepare_var var;
    var.tx_id = this->tx_id;

    int res;
    this->db->vserver->execute_prepare(var, res);
    return 0;
}

int tx_region::tx_commit() {
    // TODO: Your code here
    printf("tx[%d] commit\n", tx_id);
    chdb_protocol::commit_var var;
    var.tx_id = this->tx_id;

    int res;
    this->db->vserver->execute_commit(var, res);
    this->db->vserver->mtx.unlock();
    return 0;
}

int tx_region::tx_abort() {
    // TODO: Your code here
    printf("tx[%d] abort\n", tx_id);
    chdb_protocol::rollback_var var;
    var.tx_id = this->tx_id;

    int res;
    this->db->vserver->execute_abort(var, res);
    this->db->vserver->mtx.unlock();
    return 0;
}
