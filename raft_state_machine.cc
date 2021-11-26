#include "raft_state_machine.h"

/******************************************************************

                        Tool function

*******************************************************************/
int get_num_len(const size_t &n) {
    int number_of_digits = 0;
    size_t tmp = n;
    do {
        ++number_of_digits;
        tmp /= 10;
    } while (tmp);
    return number_of_digits;
}

void put_num(char *s, const size_t &t) {
    size_t len = (size_t)(get_num_len(t) - 1);

    size_t tmp = t;
    size_t target = 0;

    do {
        int w = tmp % 10;
        *(s + len - target) = (char) ('0' + w);

        tmp /= 10;
        ++target;
    } while (tmp);
}

void get_num(const char *s, size_t &t) {
    t = 0;
    int step = 0;
    while (*(s + step) >= '0' && *(s + step) <= '9') {
        t = t * 10;
        t += ((*(s + step)) - '0');
        ++step;
    }
    printf("Get num: %zu \n", t);
}

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

/******************************************************************

                        My code below

*******************************************************************/

kv_command::kv_command() : kv_command(CMD_NONE, "", "") {}

kv_command::kv_command(command_type tp, const std::string &key, const std::string &value) :
        cmd_tp(tp), key(key), value(value), res(std::make_shared<result>()) {
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

kv_command::kv_command(const kv_command &cmd) :
        cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), res(cmd.res) {}

kv_command::~kv_command() {}

int kv_command::size() const {
    // Your code here:
    if (cmd_tp == CMD_NONE) {
        return 0;
    }
    int ret = static_cast<int>(3 + get_num_len(key.size()) + get_num_len(value.size())
                               + key.size() + value.size());
    return ret;

}


void kv_command::serialize(char *buf_, int size) const {
    // Your code here:
    if (size != this->size() || cmd_tp == CMD_NONE) {
        return;
    }

    char *buf = buf_;
    *(buf++) = (char) ((int) cmd_tp + '0');
    put_num(buf, key.size());
    buf += get_num_len(key.size());
    *(buf++) = 'x';
    put_num(buf, value.size());
    buf += get_num_len(value.size());
    *(buf++) = 'x';

    memcpy(buf, key.c_str(), key.size());
    memcpy(buf + key.size(), value.c_str(), value.size());
    if (cmd_tp != 0)
        printf("serialize: key: %s, value: %s, cmd: %d \n", key.c_str(), value.c_str(), (int) cmd_tp);
    return;
}

void kv_command::deserialize(const char *buf, int size) {
    // Your code here:
    if (size == 0) {
        assert(0);
        return;
    }
    size_t key_size, value_size;
    cmd_tp = (command_type) (*(buf) - '0');
    get_num(buf + 1, key_size);
    get_num(buf + 2 + get_num_len(key_size), value_size);

    if (*(buf + 2 + get_num_len(key_size) + get_num_len(value_size)) != 'x'
        || *(buf + 1 + get_num_len(key_size)) != 'x') {
        printf("Wrong deserialize!");
        assert(0);
    }

    char *key_array = new char[key_size], *value_array = new char[value_size];
    memcpy(key_array, (buf + 3 + get_num_len(key_size) + get_num_len(value_size)), key_size);
    memcpy(value_array, ((buf + 3 + get_num_len(key_size) + get_num_len(value_size)) + key_size), value_size);
    key = key_size == 0 ? "" : std::string(key_array, key_size);
    value = value_size == 0 ? "" : std::string(value_array, value_size);
    delete[]key_array;
    delete[]value_array;
    return;
}

void kv_command::set_command_type(int type) {
    cmd_tp = (command_type) type;
}

marshall &operator<<(marshall &m, const kv_command &cmd) {
    // Your code here:
    m << (int) cmd.cmd_tp << cmd.key << cmd.value;
    return m;
}

unmarshall &operator>>(unmarshall &u, kv_command &cmd) {
    // Your code here:
    int tp;
    u >> tp >> cmd.key >> cmd.value;
    cmd.set_command_type(tp);
    return u;
}

kv_state_machine::~kv_state_machine() {

}





std::vector<char> kv_state_machine::snapshot() {
    // Your code here:
    mtx.lock();
    int snapshot_size = sizeof(int), n = mp.size();
    for (auto &s: mp) {
        snapshot_size += 2 * sizeof(int);
        snapshot_size += s.first.size();
        snapshot_size += s.second.size();
    }
    char *arr = new char[snapshot_size];
    int cursor = 0;
    put_int_num((arr + cursor), n);
    cursor += sizeof(int);

    for (auto &s: mp) {
        put_int_num((arr + cursor), s.first.size());
        cursor += sizeof(int);
        put_int_num((arr + cursor), s.second.size());
        cursor += sizeof(int);

        s.first.copy((arr + cursor), s.first.size());
        cursor += s.first.size();
        s.second.copy((arr + cursor), s.second.size());
        cursor += s.second.size();
    }
    assert(cursor == snapshot_size);

    mtx.unlock();
    std::string data(arr, snapshot_size);
    return std::vector<char>(data.begin(), data.end());
}

void kv_state_machine::apply_snapshot(const std::vector<char> &snapshot) {
    // Your code here:
    mtx.lock();
    std::string s(snapshot.begin(), snapshot.end());
    auto ptr = s.c_str();

    int snapshot_size, cursor = 0;
    get_int_num(ptr + cursor, snapshot_size);
    cursor += sizeof(int);
    for(int i = 0; i < snapshot_size; ++i){
        int key_s, value_s;
        get_int_num((ptr + cursor), key_s);
        cursor += sizeof(int);
        get_int_num((ptr + cursor), value_s);
        cursor += sizeof(int);

        char *k = new char[key_s], *v = new char[value_s];
        memcpy(k, (ptr + cursor), key_s);
        cursor += key_s;
        memcpy(v, (ptr + cursor), value_s);
        cursor += value_s;

        std::string key(k, key_s), value(v, value_s);
        mp.insert({key, value});
    }
    mtx.unlock();
    return;
}
void kv_state_machine::apply_log(raft_command &cmd) {
    kv_command &kv_cmd = dynamic_cast<kv_command &>(cmd);
    std::unique_lock <std::mutex> lock(kv_cmd.res->mtx);
    mtx.lock();
    // Your code here:
    switch (kv_cmd.cmd_tp) {
        case kv_command::CMD_NONE:
            break;
        case kv_command::CMD_GET:

            if (mp.count(kv_cmd.key)) {
                kv_cmd.res->succ = true;
                kv_cmd.res->key = kv_cmd.key;
                kv_cmd.res->value = mp[kv_cmd.key];
                printf("Get %s, found %s\n", kv_cmd.key.c_str(), mp[kv_cmd.key].c_str());
            } else {
                kv_cmd.res->succ = false;
                kv_cmd.res->key = kv_cmd.key;
                kv_cmd.res->value = "";
                printf("Get %s, Not found\n", kv_cmd.key.c_str());
            }
            break;
        case kv_command::CMD_DEL:
            if (mp.count(kv_cmd.key)) {
                kv_cmd.res->succ = true;
                kv_cmd.res->key = kv_cmd.key;
                kv_cmd.res->value = mp[kv_cmd.key];
                printf("DEL %s, found %s\n", kv_cmd.key.c_str(), mp[kv_cmd.key].c_str());
                mp.erase(kv_cmd.key);
            } else {
                kv_cmd.res->succ = false;
                kv_cmd.res->key = kv_cmd.key;
                kv_cmd.res->value = "";
                printf("Get %s, Not found\n", kv_cmd.key.c_str());
            }
            break;
        case kv_command::CMD_PUT:
            if (mp.count(kv_cmd.key)) {
                kv_cmd.res->succ = false;
                kv_cmd.res->key = kv_cmd.key;
                kv_cmd.res->value = mp[kv_cmd.key];

                mp[kv_cmd.key] = (kv_cmd.value);
                printf("PUT %s, found %s, replace\n", kv_cmd.key.c_str(), mp[kv_cmd.key].c_str());
            } else {
                kv_cmd.res->succ = true;
                kv_cmd.res->key = kv_cmd.key;
                kv_cmd.res->value = kv_cmd.value;
                mp.insert({kv_cmd.key, kv_cmd.value});
                printf("PUT %s, found %s\n", kv_cmd.key.c_str(), mp[kv_cmd.key].c_str());
            }
            break;
    }
    kv_cmd.res->done = true;
    kv_cmd.res->cv.notify_all();
    mtx.unlock();
    return;
}

