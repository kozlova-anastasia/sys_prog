#include "userfs.h"

#include <cstring>
#include <vector>
#include <string>
#include <algorithm>

#include "rlist.h"

namespace {

constexpr int BLOCK_SIZE = 512;
constexpr size_t MAX_FILE_SIZE = 1024 * 1024 * 100;

struct block {
    char memory[BLOCK_SIZE];
    rlist link = RLIST_LINK_INITIALIZER;

    block() {
        memset(memory, 0, BLOCK_SIZE);
    }
};

struct file {
    std::string name;
    size_t size = 0;
    int refs = 0;
    bool deleted = false;

    size_t block_count = 0;

    rlist blocks = RLIST_HEAD_INITIALIZER(blocks);
    rlist link = RLIST_LINK_INITIALIZER;
};

struct filedesc {
    file *f = nullptr;

    size_t pos = 0;
    block *cur = nullptr;
    size_t off = 0;

    bool read = true;
    bool write = true;
};

rlist file_list = RLIST_HEAD_INITIALIZER(file_list);

std::vector<filedesc*> fd_table;

ufs_error_code last_error = UFS_ERR_NO_ERR;

file* find_file(const char *name)
{
    file *f;
    rlist_foreach_entry(f, &file_list, link) {
        if (f->name == name)
            return f;
    }
    return nullptr;
}

block* first_block(file *f)
{
    if (rlist_empty(&f->blocks))
        return nullptr;
    return rlist_first_entry(&f->blocks, block, link);
}

block* next_block(file *f, block *b)
{
    if (b->link.next == &f->blocks)
        return nullptr;
    return rlist_next_entry(b, link);
}

void append_block(file *f)
{
    block *b = new block();
    rlist_add_tail(&f->blocks, &b->link);
    f->block_count++;
}

void free_blocks(file *f)
{
    while (!rlist_empty(&f->blocks)) {
        block *b = rlist_first_entry(&f->blocks, block, link);
        rlist_del(&b->link);
        delete b;
    }
    f->block_count = 0;
}

bool valid_fd(int fd)
{
    if (fd <= 0) return false;
    int i = fd - 1;
    if (i >= (int)fd_table.size()) return false;
    if (fd_table[i] == nullptr) return false;
    return true;
}

void seek_cursor(filedesc *d, size_t pos)
{
    d->pos = pos;
    d->cur = nullptr;
    d->off = 0;

    file *f = d->f;

    size_t block_id = pos / BLOCK_SIZE;
    size_t block_off = pos % BLOCK_SIZE;

    if (block_id >= f->block_count)
        return;

    block *b = first_block(f);
    for (size_t i = 0; i < block_id; i++)
        b = next_block(f, b);

    d->cur = b;
    d->off = block_off;
}

} 

ufs_error_code
ufs_errno()
{
    return last_error;
}

int
ufs_open(const char *filename, int flags)
{
    last_error = UFS_ERR_NO_ERR;

    file *f = find_file(filename);

    if (!f) {
        if (!(flags & UFS_CREATE)) {
            last_error = UFS_ERR_NO_FILE;
            return -1;
        }

        f = new file();
        f->name = filename;

        rlist_add_tail(&file_list, &f->link);
    }

    filedesc *d = new filedesc();
    d->f = f;

    seek_cursor(d, 0);

    int mask = flags & (UFS_READ_ONLY | UFS_WRITE_ONLY);

    if (mask == UFS_READ_ONLY) {
        d->read = true;
        d->write = false;
    } else if (mask == UFS_WRITE_ONLY) {
        d->read = false;
        d->write = true;
    }

    f->refs++;

    for (size_t i = 0; i < fd_table.size(); i++) {
        if (fd_table[i] == nullptr) {
            fd_table[i] = d;
            return i + 1;
        }
    }

    fd_table.push_back(d);
    return fd_table.size();
}

ssize_t
ufs_write(int fd, const char *buf, size_t size)
{
    last_error = UFS_ERR_NO_ERR;

    if (!valid_fd(fd)) {
        last_error = UFS_ERR_NO_FILE;
        return -1;
    }

    filedesc *d = fd_table[fd - 1];

    if (!d->write) {
        last_error = UFS_ERR_NO_PERMISSION;
        return -1;
    }

    if (size == 0)
        return 0;

    if (d->pos + size > MAX_FILE_SIZE) {
        last_error = UFS_ERR_NO_MEM;
        return -1;
    }

    file *f = d->f;

    size_t end = d->pos + size;

    size_t need_blocks = (end + BLOCK_SIZE - 1) / BLOCK_SIZE;

    while (f->block_count < need_blocks)
        append_block(f);

    if (!d->cur)
        seek_cursor(d, d->pos);

    size_t written = 0;

    while (written < size) {

        size_t avail = BLOCK_SIZE - d->off;
        size_t chunk = std::min(avail, size - written);

        memcpy(d->cur->memory + d->off, buf + written, chunk);

        written += chunk;
        d->pos += chunk;
        d->off += chunk;

        if (d->off == BLOCK_SIZE) {
            d->cur = next_block(f, d->cur);
            d->off = 0;
        }
    }

    f->size = std::max(f->size, end);

    return written;
}

ssize_t
ufs_read(int fd, char *buf, size_t size)
{
    last_error = UFS_ERR_NO_ERR;

    if (!valid_fd(fd)) {
        last_error = UFS_ERR_NO_FILE;
        return -1;
    }

    filedesc *d = fd_table[fd - 1];

    if (!d->read) {
        last_error = UFS_ERR_NO_PERMISSION;
        return -1;
    }

    file *f = d->f;

    if (d->pos >= f->size)
        return 0;

    size_t remain = f->size - d->pos;
    size_t to_read = std::min(remain, size);

    if (!d->cur)
        seek_cursor(d, d->pos);

    size_t read = 0;

    while (read < to_read) {

        size_t avail = BLOCK_SIZE - d->off;
        size_t chunk = std::min(avail, to_read - read);

        memcpy(buf + read, d->cur->memory + d->off, chunk);

        read += chunk;
        d->pos += chunk;
        d->off += chunk;

        if (d->off == BLOCK_SIZE) {
            d->cur = next_block(f, d->cur);
            d->off = 0;
        }
    }

    return read;
}

int
ufs_close(int fd)
{
    last_error = UFS_ERR_NO_ERR;

    if (!valid_fd(fd)) {
        last_error = UFS_ERR_NO_FILE;
        return -1;
    }

    filedesc *d = fd_table[fd - 1];
    file *f = d->f;

    delete d;
    fd_table[fd - 1] = nullptr;

    f->refs--;

    if (f->refs == 0 && f->deleted) {
        free_blocks(f);
        delete f;
    }

    return 0;
}

int
ufs_delete(const char *filename)
{
    last_error = UFS_ERR_NO_ERR;

    file *f = find_file(filename);

    if (!f) {
        last_error = UFS_ERR_NO_FILE;
        return -1;
    }

    rlist_del(&f->link);

    f->deleted = true;

    if (f->refs == 0) {
        free_blocks(f);
        delete f;
    }

    return 0;
}

#if NEED_RESIZE

int
ufs_resize(int fd, size_t new_size)
{
    last_error = UFS_ERR_NO_ERR;

    if (!valid_fd(fd)) {
        last_error = UFS_ERR_NO_FILE;
        return -1;
    }

    if (new_size > MAX_FILE_SIZE) {
        last_error = UFS_ERR_NO_MEM;
        return -1;
    }

    filedesc *d = fd_table[fd - 1];

    if (!d->write) {
        last_error = UFS_ERR_NO_PERMISSION;
        return -1;
    }

    file *f = d->f;

    if (new_size < f->size) {

		size_t need_blocks = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;

		while (f->block_count > need_blocks) {
			block *b = rlist_last_entry(&f->blocks, block, link);
			rlist_del(&b->link);
			delete b;
			f->block_count--;
		}

		for (auto d : fd_table) {
			if (!d)
				continue;
			if (d->f != f)
				continue;

			if (d->pos > new_size)
				seek_cursor(d, new_size);
		}
	}

    if (new_size > f->size) {

        size_t need_blocks = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;

        while (f->block_count < need_blocks)
            append_block(f);
    }

    f->size = new_size;

    return 0;
}

#endif

void
ufs_destroy()
{
    for (auto &d : fd_table) {
        if (d)
            delete d;
    }

    fd_table.clear();

    while (!rlist_empty(&file_list)) {
        file *f = rlist_first_entry(&file_list, file, link);
        rlist_del(&f->link);
        free_blocks(f);
        delete f;
    }
}
