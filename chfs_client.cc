// chfs client.  implements FS operations using extent server
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

chfs_client::chfs_client(std::string extent_dst)
{
    ec = new extent_client(extent_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE)
    {
        printf("isfile: %lld is a file\n", inum);
        return true;
    }
    printf("isfile: %lld is a dir\n", inum);
    return false;
}

bool chfs_client::isdir(inum inum)
{
    // used to change origin code here;
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("Problem occurs: get attr fails in isdir \n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR)
    {
        return true;
    }

    //    SYM LINK Not a dir
    return false;
}

int chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {

        printf("Problem occurs: get attr fails in GETFILE \n");
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

int chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("Problem occurs: get attr fails in GETdir \n");
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}

#define EXT_RPC(xx)                                                \
    do                                                             \
    {                                                              \
        if ((xx) != extent_protocol::OK)                           \
        {                                                          \
            printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
            r = IOERR;                                             \
            goto release;                                          \
        }                                                          \
    } while (0)

// Only support set size of attr
int chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    std::string buf;

    //    Bugs!!!
    //    Can't change time!!!
    //    if (buf.size() == size) {
    //        return OK;
    //    }

    if (ec->get(ino, buf) != extent_protocol::OK)
    {
        printf("Problem occurs: read buf fails in setattr \n");
        return IOERR;
    }

    buf.resize(size);

    if (ec->put(ino, buf) != extent_protocol::OK)
    {
        printf("Problem occurs: write buf fails in setattr \n");
        return IOERR;
    }

    return r;
}

/**
 *
 * @param name name of file name entry
 * @param wait4write the data structure stored in blocks
 */
void chfs_client::setEntry(const char *name, inum inode, real_dirent_in_blocks &entry)
{
    //    Set inode number and name length
    entry.file_name_length = strlen(name);
    entry.inum = inode;

    memcpy((entry.name), name, strlen(name));
    return;
}

int chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
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
    if (lookup(parent, name, found, ino_out) != OK)
    {
        printf("Problem occurs: lookup fails in create \n");
        return IOERR;
    }

    if (found)
    {
        printf("Problem occurs: EXIST filename in create \n");
        return EXIST;
    }

    if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK)
    {
        printf("Problem occurs: ec creates file fail in create \n");
        return IOERR;
    }

    if (ec->get(parent, buf) != extent_protocol::OK)
    {
        printf("Problem occurs: read directory fails in create \n");
        return IOERR;
    }

    struct real_dirent_in_blocks entry;
    setEntry(name, ino_out, entry);
    buf.append((char *)(&entry), sizeof(real_dirent_in_blocks));

    if (ec->put(parent, buf) != extent_protocol::OK)
    {
        printf("Problem occurs: write directory fails in create \n");
        return IOERR;
    }

    return r;
}

int chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool found = false;
    std::string buf;

    if (lookup(parent, name, found, ino_out) != OK)
    {
        printf("Problem occurs: dup dir name in mkdir \n");
        return IOERR;
    }

    if (found)
    {
        printf("Problem occurs: dup dir name in mkdir \n");
        return EXIST;
    }

    if (ec->get(parent, buf) != extent_protocol::OK)
    {
        printf("Problem occurs: read parent fails in mkdir \n");
        return IOERR;
    }

    if (ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK)
    {
        printf("Problem occurs: create dir fails in mkdir \n");
        return IOERR;
    }

    struct real_dirent_in_blocks entry;
    setEntry(name, ino_out, entry);
    buf.append((char *)(&entry), sizeof(real_dirent_in_blocks));

    if (ec->put(parent, buf) != extent_protocol::OK)
    {
        printf("Problem occurs: write parent fails in mkdir \n");
        return IOERR;
    }
    return r;
}

int chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    std::list<dirent> l;
    if (!isdir(parent))
    {
        printf("Problem occurs: Wrong file type in readdir \n");
        return IOERR;
    }

    if (readdir(parent, l) != OK)
    {
        printf("Problem occurs: readdir fails in lookup \n");
        return IOERR;
    }

    while (!l.empty())
    {
        dirent ent = l.front();
        l.pop_front();

        if (ent.name == std::string(name))
        {
            found = true;
            ino_out = ent.inum;
            return OK;
        }
    }

    // NOT Found here
    found = false;
    return OK;
}

int chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    std::string buf;

    // check type == DIR here
    if (!isdir(dir))
    {
        printf("Problem occurs: parent not a dic in readdir \n");
        return IOERR;
    }

    if (ec->get(dir, buf) != extent_protocol::OK)
    {
        printf("Problem occurs: read buf fails in readdir \n");
        return IOERR;
    }

    size_t size_dirent = sizeof(struct real_dirent_in_blocks);
    unsigned int entry_number = buf.size() / size_dirent;
    const char *cursor = buf.c_str();

    for (unsigned int i = 0; i < entry_number; ++i)
    {
        struct real_dirent_in_blocks entry;
        memcpy((void *)&entry, cursor + (i * size_dirent), size_dirent);

        struct dirent s;
        s.inum = entry.inum;
        s.name.assign(entry.name, entry.file_name_length);

        list.push_back(s);
    }

    return r;
}

int chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string buf = "";

    if (ec->get(ino, buf) != extent_protocol::OK)
    {
        printf("Problem occurs: get data fails in read \n");
        return IOERR;
    }
    if ((long int)buf.size() <= off)
    {
        printf("Problem occurs: read offset is larger than size in read \n");
        return IOERR;
    }

    data.clear();
    data = buf.substr(off, size);

    return r;
}

int chfs_client::write(inum ino, size_t size, off64_t off, const char *data,
                       size_t &bytes_written)
{
    int r = OK;
    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    std::string buf = "", data_str = std::string(data, size);

    if (ec->get(ino, buf) != extent_protocol::OK)
    {
        printf("Problem occurs: get data fails in write \n");
        return IOERR;
    }

    // function caller has to check size
    if (size > data_str.size())
    {
        printf("Problem occurs: write size (%zu) is larger than provided(%lu) in write \n", size, data_str.size());
        return IOERR;
    }

    printf("write: size=%zu, off=%ld, byte size = %lu", size, off, data_str.size());

    bytes_written = size;

    if (buf.size() <= (unsigned int)(((unsigned int)off) + size))
    {
        buf.resize((size_t)(((unsigned int)off) + size), '\0');
    }
    buf.replace(((unsigned int)off), size, data_str);

    if (ec->put(ino, buf) != extent_protocol::OK)
    {
        printf("Problem occurs: write back data fails in write \n");
        return IOERR;
    }
    return r;
}

int chfs_client::unlink(inum parent, const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    bool found = false;
    std::string buf;
    inum ino_out = 0;

    if (lookup(parent, name, found, ino_out) != OK)
    {
        printf("Problem occurs: read parent fails in unlink \n");
        return IOERR;
    }

    if (!found)
    {
        printf("Problem occurs: not found name in unlink \n");
        return NOENT;
    }

    if (ec->remove(ino_out) != extent_protocol::OK)
    {
        printf("Problem occurs: delete node fails in unlink \n");
        return IOERR;
    }

    std::list<dirent> l;
    if (readdir(parent, l) != extent_protocol::OK)
    {
        printf("Problem occurs: get l fails in unlink \n");
        return IOERR;
    }

    std::string new_dic;
    while (!l.empty())
    {
        struct dirent entry = l.front();
        l.pop_front();

        if (entry.inum != ino_out)
        {
            real_dirent_in_blocks s;
            setEntry(entry.name.c_str(), entry.inum, s);
            new_dic.append((char *)(&s), sizeof(struct real_dirent_in_blocks));
        }
    }

    if (ec->put(parent, new_dic) != extent_protocol::OK)
    {
        printf("Problem occurs: write parent fails in unlink \n");
        return IOERR;
    }
    return r;
}

/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 *
 * */

bool chfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("Problem occurs: get attr fails in issym \n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK)
    {
        return true;
    }
    return false;
}

/**
 *
 * @param node  inum
 * @param buf  content of link
 * @return
 */
int chfs_client::read_link(inum node, std::string &buf)
{

    if (ec->get(node, buf) != extent_protocol::OK)
    {
        printf("Problem occurs: get parent fails in read link \n");
        return IOERR;
    }
    return OK;
}

/**
 * this function link dest to parent/name
 * @param link
 * @return chfs::status
 */
int chfs_client::symlink(inum parent, const char *name, const char *dest, inum &inode)
{
    std::string buf;

    if (ec->get(parent, buf) != extent_protocol::OK)
    {
        printf("Problem occurs: write parent fails in symlink \n");
        return IOERR;
    }

    bool found = false;
    inum tmp = 0;
    if (lookup(parent, name, found, tmp) != OK)
    {
        printf("Problem occurs: lookup fails in symlink \n");
        return IOERR;
    }

    if (found)
    {
        printf("Problem occurs: dup dir name in symlink \n");
        return EXIST;
    }

    if (ec->create(extent_protocol::T_SYMLINK, inode) != extent_protocol::OK)
    {
        printf("Problem occurs: create soft link fails in symlink \n");
        return IOERR;
    }

    if (ec->put(inode, std::string(dest)) != extent_protocol::OK)
    {
        printf("Problem occurs: write symlink into buf fails in symlink \n");
        return IOERR;
    }

    struct real_dirent_in_blocks entry;
    setEntry(name, inode, entry);
    buf.append((char *)(&entry), sizeof(struct real_dirent_in_blocks));

    if (ec->put(parent, buf) != extent_protocol::OK)
    {
        printf("Problem occurs: write parent fails in symlink \n");
        return IOERR;
    }

    return OK;
}