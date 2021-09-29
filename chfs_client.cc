// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <list>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client() {
    ec = new extent_client();

}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst) {
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n) {
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum) {
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum) {
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    }
    printf("isfile: %lld is a dir\n", inum);
    return false;
}

/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
chfs_client::isdir(inum inum) {
    // Oops! is this still correct when you implement symlink?
    return !isfile(inum);
}

int
chfs_client::getfile(inum inum, fileinfo &fin) {
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

    release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din) {
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

    release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
chfs_client::setattr(inum ino, size_t size) {
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    std::string buf;
    extent_protocol::attr a;

    if (ec->getattr(ino, a) != extent_protocol::OK) {
        printf("Problem occurs: get attr fails in setattr \n");
        return IOERR;
    }

    if (a.size == size) {
        return OK;
    }

    if (ec->get(ino, buf) != extent_protocol::OK) {
        printf("Problem occurs: read buf fails in setattr \n");
        return IOERR;
    }

    if (a.size < size) {
        std::string str = std::string(size - a.size, '\0');
        buf.insert(buf.end(), str.begin(), str.end());
    }

    size_t written_size = 0;
    if (write(ino, size, 0, buf.c_str(), written_size) != OK) {
        printf("Problem occurs: write buf to reduce size fails in setattr \n");
        return IOERR;
    }

    if (written_size != size) {
        printf("Problem occurs: write buf size != Predicted size in setattr \n");
        return IOERR;
    }

    return r;
}

int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out) {
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

//    We will check parent is dir in lookup function
//    extent_protocol::attr parent_attr;
//    if (ec->getattr(parent, parent_attr) != extent_protocol::OK) {
//        printf("Problem occurs: get attr fails in create \n");
//        return IOERR;
//    }
//    else if(parent_attr.type != extent_protocol::T_DIR){
//        printf("Problem occurs: parent not a dir in create \n");
//        return IOERR;
//    }

    std::string buf;
    bool found = false;

    // Test legacy of name and parent inum
    inum inode_out = 0;
    if (lookup(parent, name, found, inode_out) == OK && found) {
        printf("Problem occurs: EXIST filename in create \n");
        return EXIST;
    }

    if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK) {
        printf("Problem occurs: ec creates file fail in create \n");
        return IOERR;
    }

    if (ec->get(parent, buf) != extent_protocol::OK) {
        printf("Problem occurs: read directory fails in create \n");
        return IOERR;
    }

    struct real_dirent_in_blocks entry;

    entry.file_name_length = strlen(name);
    memcpy((void*)(&entry.name), name, entry.file_name_length);
    entry.inum = ino_out;

    buf.append((char *) (&entry), sizeof(struct real_dirent_in_blocks));

    if (ec->put(parent, buf) != extent_protocol::OK) {
        printf("Problem occurs: write directory fails in create \n");
        return IOERR;
    }

    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out) {
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool found = false;
    std::string buf;
    if (lookup(parent, name, found, ino_out) == extent_protocol::OK && found) {
        printf("Problem occurs: dup dir name in mkdir \n");
        return IOERR;
    }

    if (ec->get(parent, buf) != extent_protocol::OK) {
        printf("Problem occurs: read parent fails in mkdir \n");
        return IOERR;
    }

    if (ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK) {
        printf("Problem occurs: create dir fails in mkdir \n");
        return IOERR;
    }

    // TODO: What is your dir format?
    struct real_dirent_in_blocks entry;

    entry.file_name_length = strlen(name);
    memcpy((void*)(&entry.name), name, entry.file_name_length);
    entry.inum = ino_out;

    buf.append((char *) (&entry), sizeof(struct real_dirent_in_blocks));

    if (ec->put(parent, buf) != extent_protocol::OK) {
        printf("Problem occurs: write parent fails in mkdir \n");
        return IOERR;
    }
    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out) {
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    std::list <dirent> l;
    extent_protocol::attr attr;

    if (ec->getattr(parent, attr) != extent_protocol::OK) {
        printf("Problem occurs: getattr fails in lookup \n");
        return IOERR;
    }

    if (attr.type != extent_protocol::T_DIR) {
        printf("Problem occurs: Wrong file type in readdir \n");
        return IOERR;
    }

    if (readdir(parent, l) != extent_protocol::OK) {
        printf("Problem occurs: readdir fails in lookup \n");
        return IOERR;
    }

    while (!l.empty()) {
        dirent ent = l.front();
        l.pop_front();

        if (ent.name == std::string(name)) {
            found = true;
            ino_out = ent.inum;
            return OK;
        }
    }

    // NOT Found here
    found = false;
    return r;
}

int
chfs_client::readdir(inum dir, std::list <dirent> &list) {
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    std::string buf;
    extent_protocol::attr a;

    if (ec->getattr(dir, a) != extent_protocol::OK) {
        printf("Problem occurs: get attr fails in readdir \n");
        return IOERR;
    }

    // check type == DIR here
    if (a.type != extent_protocol::T_DIR) {
        printf("Problem occurs: parent not a dic in readdir \n");
        return IOERR;
    }

    if (ec->get(dir, buf) != extent_protocol::OK) {
        printf("Problem occurs: read buf fails in readdir \n");
        return IOERR;
    }
    size_t size_dirent = sizeof(struct real_dirent_in_blocks);
    unsigned int entry_number = buf.size() / size_dirent;

    for (uint32_t i = 0; i < entry_number; ++i) {
        struct real_dirent_in_blocks entry;
        memcpy((void *) &entry, buf.c_str() + (i * size_dirent), size_dirent);

        struct dirent s;
        memcpy((void*)(&s.name), &entry.name, entry.file_name_length);
        s.inum = entry.inum;
        list.push_back(s);
    }

    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data) {
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */

    extent_protocol::attr attr;
    std::string buf = "";

    if (ec->getattr(ino, attr) != extent_protocol::OK) {
        printf("Problem occurs: get attr fails in read \n");
        return IOERR;
    }
    if (ec->get(ino, buf) != extent_protocol::OK) {
        printf("Problem occurs: get data fails in read \n");
        return IOERR;
    }
    if (attr.size <= off) {
        printf("Problem occurs: read offset is larger than size in read \n");
        return IOERR;
    }

    data = buf.substr(off, size);
    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
                   size_t &bytes_written) {
    int r = OK;
    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    std::string buf = "", data_str = std::string(data);

    if (size > data_str.size()) {
        printf("Problem occurs: write size is larger than provided in write \n");
        return IOERR;
    }

    if (ec->get(ino, buf) != extent_protocol::OK) {
        printf("Problem occurs: get data fails in write \n");
        return IOERR;
    }

    if ((long unsigned int) off <= buf.size()) {
        bytes_written = size;
    } else {
        bytes_written = (off - buf.size() + size);
        std::string tmp = std::string(off - buf.size(), '\0');
        data_str.insert(data_str.begin(), tmp.begin(), tmp.end());
    }

    buf.insert(buf.begin() + off, data_str.begin(), data_str.end());

    if (ec->put(ino, buf) != extent_protocol::OK) {
        printf("Problem occurs: write back data fails in write \n");
        return IOERR;
    }
    return r;
}

int chfs_client::unlink(inum parent, const char *name) {
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    bool found = false;
    std::string buf;
    inum ino_out = 0;
    if (lookup(parent, name, found, ino_out) != extent_protocol::OK) {
        printf("Problem occurs: read parent fails in unlink \n");
        return IOERR;
    }

    if(!found){
        printf("Problem occurs: No such a file to unlink \n");
        return NOENT;
    }

    if (ec->get(parent, buf) != extent_protocol::OK) {
        printf("Problem occurs: read parent fails in unlink \n");
        return IOERR;
    }

    if (ec->remove(ino_out) != extent_protocol::OK || ec->remove(parent) != extent_protocol::OK) {
        printf("Problem occurs: delete node fails in unlink \n");
        return IOERR;
    }

    // TODO: What is your dic format here?
    size_t size_dirent = sizeof(struct real_dirent_in_blocks);
    unsigned int entry_number = buf.size() / size_dirent;

    std::string new_dic;
    size_t cursor = 0;
    for (uint32_t i = 0; i < entry_number; ++i) {
        struct real_dirent_in_blocks entry;
        memcpy((void *) &entry, buf.c_str() + (i * size_dirent), size_dirent);

        if(entry.inum != ino_out){
            memcpy((void*)(new_dic.c_str() + cursor), &entry, size_dirent);
            cursor += size_dirent;
        }
    }

    if (ec->put(parent, new_dic) != extent_protocol::OK) {
        printf("Problem occurs: write parent fails in unlink \n");
        return IOERR;
    }
    return r;
}

// TODO: Handle symbolic link!


