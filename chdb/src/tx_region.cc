#include "tx_region.h"

void tx_region::sendPutRPC(int key, int val, int &res, bool extra) {
    chdb_protocol::operation_var var;
    var.tx_id = this->tx_id;
    var.key = key;
    var.value = val;

    this->db->vserver->execute(key, chdb_protocol::Put, var, res);
    if(extra) {
        logs.push_back(var);
    }
}

void tx_region::sendGetRPC(int key, int &res) {
    chdb_protocol::operation_var var;
    var.tx_id = this->tx_id;
    var.key = key;

    this->db->vserver->execute(key, chdb_protocol::Get, var, res);
    var.value = INT32_MAX;
    logs.push_back(var);
}

int tx_region::put(const int key, const int val) {
    // TODO: Your code here
#if PART3
    int res;
    db->latch.lock();
    if(!this->db->locks.count(key)) {
        db->locks[key] = std::make_shared<std::mutex>();
        db->owner[key] = this->tx_id;
    }
    db->latch.unlock();

    // try it now
    bool stopped = false;
    while(!stopped) {
        db->latch.lock();
        if(db->locks[key]->try_lock() || db->owner[key] == tx_id) {
            db->owner[key] = this->tx_id;
            sendPutRPC(key, val, res, true);
            stopped = true;

        } else if(db->owner[key] > this->tx_id) {
            db->latch.unlock();
            db->locks[key]->lock();
            db->latch.lock();
            db->owner[key] = this->tx_id;
            sendPutRPC(key, val, res, true);
            stopped = true;
        }
        db->latch.unlock();
        // we can't get lock!!
        if(!stopped) {
            tx_abort();
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            retry();
        }
    }

#else
    int res;
    chdb_protocol::operation_var var;
    var.tx_id = this->tx_id;
    var.key = key;
    var.value = val;

    this->db->vserver->execute(key, chdb_protocol::Put, var, res);
#endif

    return 0;
}

int tx_region::get(const int key) {
    // TODO: Your code here.
#if PART3
    int res;
    db->latch.lock();
    if(!this->db->locks.count(key)) {
        db->locks[key] = std::make_shared<std::mutex>();
        db->owner[key] = this->tx_id;
    }
    db->latch.unlock();

    // try it now
    bool stopped = 0;
    while(!stopped) {
        db->latch.lock();
        if(db->locks[key]->try_lock() || db->owner[key] == tx_id) {
            db->owner[key] = this->tx_id;
            sendGetRPC(key, res);
            stopped = true;

        } else if(db->owner[key] > this->tx_id) {
            db->latch.unlock();
            db->locks[key]->lock();
            db->latch.lock();
            db->owner[key] = this->tx_id;
            sendGetRPC(key, res);
            stopped = true;
        }
        db->latch.unlock();
        // we can't get lock!!
        if(!stopped) {
            tx_abort();
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            retry(); // retry as logs instruct
        }
    }
#else
    int res;
    chdb_protocol::operation_var var;
    var.tx_id = this->tx_id;
    var.key = key;

    this->db->vserver->execute(key, chdb_protocol::Get, var, res);
#endif
    return res;
}

int tx_region::tx_can_commit() {
    // TODO: Your code here
    chdb_protocol::check_prepare_state_var var;
    var.tx_id = this->tx_id;

    int res;
    this->db->vserver->execute_check(var, res);

    if(res) {
        std::set<int> keys;
        for (auto &log: logs) {
            keys.insert(log.key);
        }
        db->latch.lock();
        for (auto &key: keys) {
            if (db->owner[key] != this->tx_id) {
                return false;
            }
        }
        db->latch.unlock();
    }
    return res;
}

int tx_region::tx_begin() {
    // TODO: Your code here
    printf("tx[%d] begin\n", tx_id);
#if BIG_LOCK
    this->db->vserver->mtx.lock();
#endif

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

#if BIG_LOCK
    this->db->vserver->mtx.unlock();
#elif PART3
    release();
#endif

    return 0;
}

int tx_region::tx_abort() {
    // TODO: Your code here
    printf("tx[%d] abort\n", tx_id);
    chdb_protocol::rollback_var var;
    var.tx_id = this->tx_id;

    int res;
    this->db->vserver->execute_abort(var, res);

#if BIG_LOCK
    this->db->vserver->mtx.unlock();
#elif PART3
    release();
#endif

    return 0;
}

#if PART3

void tx_region::retry() {
    bool stopped = false;
    while (!stopped) { // get All locks we need before fail until success
        stopped = true;
        std::set<int> own_locks;
        db->latch.lock();

        for(auto &log: logs) {
            if(!own_locks.count(log.key)) {
                if(db->locks[log.key]->try_lock()) {
                    db->owner[log.key] = this->tx_id;
                    own_locks.insert(log.key);
                } else if(db->owner[log.key] > this->tx_id) {
                    db->owner[log.key] = this->tx_id;
                    own_locks.insert(log.key);
                } else {
                    stopped = false;
                    for(auto own_lock: own_locks) {
                        db->owner[own_lock] = INT32_MAX;
                        db->locks[own_lock]->unlock();
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    break;
                }
            }
        }
        db->latch.unlock();
    }

    // retry all put
    for(auto &log: logs) {
        if(log.value != INT32_MAX) {
            int res;
            sendPutRPC(log.key, log.value, res, false);
        }
    }
}

void tx_region::release() {
    std::set<int> keys;
    for(auto &log: logs) {
        keys.insert(log.key);
    }
    db->latch.lock();
    for(auto &key: keys) {
        db->locks[key]->unlock();
        db->owner[key] = INT32_MAX;
    }
    db->latch.unlock();
}

#endif