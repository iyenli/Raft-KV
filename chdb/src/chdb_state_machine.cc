#include "chdb_state_machine.h"

/******************************************************************

                        Tool function

*******************************************************************/
void put_int_num(char *buf, const int &value) {
    assert(sizeof(int) == 4);
    buf[0] = (value >> 24) & 0xff;
    buf[1] = (value >> 16) & 0xff;
    buf[2] = (value >> 8) & 0xff;
    buf[3] = value & 0xff;
}

void get_int_num(const char *buf, int &value) {
    assert(sizeof(int) == 4);
    value = (buf[0] & 0xff) << 24;
    value |= (buf[1] & 0xff) << 16;
    value |= (buf[2] & 0xff) << 8;
    value |= buf[3] & 0xff;
}

chdb_command::chdb_command() {
    // TODO: Your code here
    this->cmd_tp = CMD_NONE;
    this->res = std::make_shared<result>();
}

chdb_command::chdb_command(command_type tp, const int &key, const int &value, const int &tx_id)
        : cmd_tp(tp), key(key), value(value), tx_id(tx_id), res(std::make_shared<result>()) {
    // TODO: Your code here
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

chdb_command::chdb_command(const chdb_command &cmd) :
        cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), tx_id(cmd.tx_id), res(std::move(cmd.res)) {
    // TODO: Your code here

}


void chdb_command::serialize(char *buf, int size) const {
    // TODO: Your code here
    if (this->cmd_tp == CMD_NONE) {
        return;
    }

    int cursor = 0;
    put_int_num((buf + cursor), (int) (cmd_tp));
    cursor += sizeof(int);
    put_int_num((buf + cursor), this->key);
    cursor += sizeof(int);
    put_int_num((buf + cursor), this->value);
    cursor += sizeof(int);
    put_int_num((buf + cursor), this->tx_id);
}

void chdb_command::deserialize(const char *buf, int size) {
    // TODO: Your code here
    int cursor = 0, cmd_int_tp = 0;

    get_int_num((buf + cursor), cmd_int_tp);
    cmd_tp = (command_type) cmd_int_tp;
    cursor += sizeof(int);
    get_int_num((buf + cursor), this->key);
    cursor += sizeof(int);
    get_int_num((buf + cursor), this->value);
    cursor += sizeof(int);
    get_int_num((buf + cursor), this->tx_id);
}

marshall &operator<<(marshall &m, const chdb_command &cmd) {
    // TODO: Your code here\

    m << (int) cmd.cmd_tp << cmd.value << cmd.key << cmd.tx_id;
    return m;
}

unmarshall &operator>>(unmarshall &u, chdb_command &cmd) {
    // TODO: Your code here
    int tp_int;
    u >> tp_int >> cmd.value >> cmd.key >> cmd.tx_id;
    cmd.set_command_type(tp_int);
    return u;
}

void chdb_state_machine::apply_log(raft_command &cmd) {
    // TODO: Your code here
    chdb_command command = dynamic_cast<chdb_command &>(cmd);
    mtx.lock();
    if (!mp.count(command.tx_id)) {
        mp.insert({command.tx_id,
                   std::vector < std::pair < chdb_command::command_type, std::vector < int>>>()});
    }
    mp[command.tx_id].push_back(std::make_pair(command.cmd_tp,
                                               std::vector<int>({command.key, command.value})));
    command.res->key = command.key;
    command.res->tx_id = command.tx_id;
    command.res->value = command.value;
    command.res->done = true;
    command.res->cv.notify_all();
    mtx.unlock();
}