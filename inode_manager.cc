#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk() {
    bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf) {

    std::memcpy(buf, blocks[id], BLOCK_SIZE);
    return;
}

void
disk::write_block(blockid_t id, const char *buf) {

    int len = strlen(buf);

    len = len > BLOCK_SIZE ? BLOCK_SIZE : len;
    std::memcpy(blocks[id], buf, BLOCK_SIZE);
    return;
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block() {
    /*
     * your code goes here.
     * note: you should mark the corresponding bit in block bitmap when alloc.
     * you need to think about which block you can start to be allocated.
     */
    for (int i = MAXFILE; i <= BLOCK_NUM; ++i) {
        if (using_blocks[i] == 0) {
            using_blocks[i] = 1;
            return i;
        }
    }
    return 0;
}

void
block_manager::free_block(uint32_t id) {
    /*
     * your code goes here.
     * note: you should unmark the corresponding bit in the block bitmap when free.
     */

    if (using_blocks[id] == 0) {
        printf("you r write an unused block to 0 \n");
        return;
    }

    using_blocks[id] = 0;
    return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager() {
    d = new disk();

    // format the disk
    sb.size = BLOCK_SIZE * BLOCK_NUM;
    sb.nblocks = BLOCK_NUM;
    sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf) {
    d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf) {
    d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager() {
    bm = new block_manager();
    uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
    if (root_dir != 1) {
        printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
        exit(0);
    }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type) {
    /*
     * your code goes here.
     * note: the normal inode block should begin from the 2nd inode block.
     * the 1st is used for root_dir, see inode_manager::inode_manager().
     */

    static int cursor = 0;

    for (int i = 0; i < INODE_NUM; ++i) {
        cursor = (cursor + 1) % INODE_NUM;
        if (cursor == 0)
            continue;

        inode_t *node = get_inode(cursor);
        if (node != NULL)
            continue;

        node = (inode_t *) malloc(sizeof(inode_t));
        bzero(node, sizeof(inode_t));
        node->type = type;
        node->size = 0;
        node->ctime = time(NULL);
        node->mtime = time(NULL);
        node->atime = time(NULL);

        put_inode(cursor, node);
        free(node);

        break;
    }

    return cursor;
}

void
inode_manager::free_inode(uint32_t inum) {
    /*
     * your code goes here.
     * note: you need to check if the inode is already a freed one;
     * if not, clear it, and remember to write back to disk.
     */

    if (inum < 0 || inum >= INODE_NUM) {
        printf("Problem occurs: free_inode meet a wrong inum \n");
        return;
    }

    inode *node = get_inode(inum);
    if (node == nullptr)
        return;

    if (node->type == 0) {
        printf("Problem occurs: try to free an unused inode \n");
        return;
    }

    node->type = 0;
    put_inode(inum, node);
    return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *
inode_manager::get_inode(uint32_t inum) {
    struct inode *ino, *ino_disk;
    char buf[BLOCK_SIZE];

    printf("\tim: get_inode %d\n", inum);

    if (inum < 0 || inum >= INODE_NUM) {
        printf("\tim: inum out of range\n");
        return NULL;
    }

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    // printf("%s:%d\n", __FILE__, __LINE__);

    ino_disk = (struct inode *) buf + inum % IPB;
    if (ino_disk->type == 0) {
        printf("\tim: inode not exist\n");
        return NULL;
    }

    ino = (struct inode *) malloc(sizeof(struct inode));
    *ino = *ino_disk;

    return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino) {
    char buf[BLOCK_SIZE];
    struct inode *ino_disk;

    printf("\tim: put_inode %d\n", inum);
    if (ino == NULL)
        return;

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino_disk = (struct inode *) buf + inum % IPB;
    *ino_disk = *ino;
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a, b) ((a)<(b) ? (a) : (b))
#define GET_BLOCK_NUM_BY_SIZE(size) (size % BLOCK_SIZE == 0? size / BLOCK_SIZE: size / BLOCK_SIZE + 1)

blockid_t
inode_manager::find_block_by_index(int index, inode *node) {

    if (index < 0)
        return 0;

    if (node == nullptr)
        return 0;

    if (index < NDIRECT)
        return node->blocks[index];
    else if ((unsigned long) index < MAXFILE) {
        char buf[BLOCK_SIZE];
        bm->read_block(node->blocks[NDIRECT], buf);
        return ((blockid_t *) buf)[index - NDIRECT];
    }

    exit(0);
}

void
inode_manager::alloc_new_block(int tol, inode *node) {

    if (node == nullptr)
        return;

    if (tol < NDIRECT)
        node->blocks[tol] = bm->alloc_block();
    else if ((unsigned long) tol < MAXFILE) {
        if (!node->blocks[NDIRECT]) {
            node->blocks[NDIRECT] = bm->alloc_block();
        }
        char buf[BLOCK_SIZE];
        bm->read_block(node->blocks[NDIRECT], buf);
        ((blockid_t *) buf)[tol - NDIRECT] = bm->alloc_block();
        bm->write_block(node->blocks[NDIRECT], buf);
    }
}

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size) {
    /*
     * your code goes here.
     * note: read blocks related to inode number inum,
     * and copy them to buf_out
     */

    inode *node = get_inode(inum);
    if (node == nullptr)
        return;

    //  We can' handle larger size.
    if (node->size >= BLOCK_SIZE * MAXFILE)
        return;

    *size = node->size;

    *buf_out = new char[*size];
    int max_block = *size / BLOCK_SIZE;
    int cursor = 0;

    char buf[BLOCK_SIZE];
    for (int i = 0; i < max_block; ++i) {
        bm->read_block(find_block_by_index(i, node), buf);
        std::memcpy(((*buf_out) + cursor), buf, BLOCK_SIZE);
        cursor += BLOCK_SIZE;
    }

    if (cursor < *size) {
        bm->read_block(find_block_by_index(max_block, node), buf);
        std::memcpy(((*buf_out) + cursor), buf, *size - cursor);
    }

    free(node);

    return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size) {
    /*
     * your code goes here.
     * note: write buf to blocks of inode inum.
     * you need to consider the situation when the size of buf
     * is larger or smaller than the size of original inode
     */
    inode *node = get_inode(inum);

    if (node == NULL) {
        printf("Problem occurs: write_file meets NULL \n");
        return;
    }

    if ((unsigned int) size >= BLOCK_SIZE * MAXFILE || size < 0)
        return;

//    cursor indicates the cursor of buffer
    int cursor = 0;

    int has_block = 0;
    if (node->size > 0)
        has_block = (node->size - 1) / BLOCK_SIZE + 1;

    int new_block = 0;
    if (size > 0)
        new_block = (size - 1) / BLOCK_SIZE + 1;

    printf("\t %d, %d, %d, %d \n", inum, node->size, has_block, new_block);
    if (has_block > new_block) {
        for (int i = new_block; i < has_block; ++i) {
            bm->free_block(find_block_by_index(i, node));
        }
    } else if (has_block < new_block) {
        for (int i = has_block; i < new_block; ++i) {
            alloc_new_block(i, node);
        }
    }

    new_block = size / BLOCK_SIZE;

    for (int i = 0; i < new_block; ++i) {
        bm->write_block(find_block_by_index(i, node), buf + cursor);
        cursor += BLOCK_SIZE;
    }

    if (cursor < size) {
        char buf_tmp[BLOCK_SIZE];
        memcpy(buf_tmp, buf + cursor, size - cursor);
        bm->write_block(find_block_by_index(new_block, node), buf_tmp);
    }

    printf("%d", size);
    node->size = size;
    node->ctime = time(NULL);
    node->atime = time(NULL);
    node->mtime = time(NULL);
    put_inode(inum, node);

    free(node);
    return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a) {
    /*
     * your code goes here.
     * note: get the attributes of inode inum.
     * you can refer to "struct attr" in extent_protocol.h
     */
    inode *ret = get_inode(inum);
    if (ret == nullptr)
        return;

    a.type = ret->type;
    a.atime = ret->atime;
    a.ctime = ret->ctime;
    a.mtime = ret->mtime;
    a.size = ret->size;

    free(ret);
    return;
}

void
inode_manager::remove_file(uint32_t inum) {
    /*
     * your code goes here
     * note: you need to consider about both the data block and inode of the file
     */

    if (inum < 0 || inum >= INODE_NUM)
        return;

    inode *node = get_inode(inum);
    if (node == nullptr)
        return;


    int block_num = 0;
    if (node->size != 0)
        block_num = (node->size - 1) / BLOCK_SIZE;

//    Free data and secondary level block
    for (int i = 0; i < block_num; ++i) {
        bm->free_block(find_block_by_index(i, node));
    }

    if (block_num > NDIRECT)
        bm->free_block(node->blocks[NDIRECT]);

    bzero(node, sizeof(inode_t));
    free_inode(inum);
    free(node);
    return;
}
