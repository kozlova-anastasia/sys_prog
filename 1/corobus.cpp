#include "corobus.h"

#include "libcoro.h"
#include "rlist.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <deque>

struct wakeup_entry {
    struct rlist base;
    struct coro *coro;
};

struct wakeup_queue {
    struct rlist coros;
};

static void
wakeup_queue_init(struct wakeup_queue *queue)
{
    rlist_create(&queue->coros);
}

static void
wakeup_queue_suspend_this(struct wakeup_queue *queue)
{
    struct wakeup_entry entry;
    entry.coro = coro_this();
    rlist_add_tail_entry(&queue->coros, &entry, base);
    coro_suspend();
    rlist_del_entry(&entry, base);
}

static void
wakeup_queue_wakeup_first(struct wakeup_queue *queue)
{
    if (rlist_empty(&queue->coros))
        return;

    struct wakeup_entry *entry =
        rlist_first_entry(&queue->coros, struct wakeup_entry, base);

    rlist_del_entry(entry, base);   // 🔥 ЭТОЙ СТРОКИ У ТЕБЯ НЕТ

    coro_wakeup(entry->coro);
}

struct coro_bus_channel {
    size_t size_limit;

    struct wakeup_queue send_queue;
    struct wakeup_queue recv_queue;

    std::deque<unsigned> data;
};

struct coro_bus {
    struct coro_bus_channel **channels;
    int channel_count;
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code
coro_bus_errno(void)
{
    return global_error;
}

void
coro_bus_errno_set(enum coro_bus_error_code err)
{
    global_error = err;
}

struct coro_bus *
coro_bus_new(void)
{
    struct coro_bus *bus = new coro_bus;

    bus->channels = NULL;
    bus->channel_count = 0;

    return bus;
}

void
coro_bus_delete(struct coro_bus *bus)
{
    for (int i = 0; i < bus->channel_count; i++) {
        if (bus->channels[i] != NULL)
            delete bus->channels[i];
    }

    delete[] bus->channels;
    delete bus;
}

int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit)
{
    for (int i = 0; i < bus->channel_count; i++) {
        if (bus->channels[i] == NULL) {
            struct coro_bus_channel *ch = new coro_bus_channel;
            ch->size_limit = size_limit;

            wakeup_queue_init(&ch->send_queue);
            wakeup_queue_init(&ch->recv_queue);

            bus->channels[i] = ch;
            return i;
        }
    }

    struct coro_bus_channel **new_arr =
        new coro_bus_channel *[bus->channel_count + 1];

    for (int i = 0; i < bus->channel_count; i++)
        new_arr[i] = bus->channels[i];

    new_arr[bus->channel_count] = new coro_bus_channel;

    struct coro_bus_channel *ch = new_arr[bus->channel_count];
    ch->size_limit = size_limit;

    wakeup_queue_init(&ch->send_queue);
    wakeup_queue_init(&ch->recv_queue);

    delete[] bus->channels;

    bus->channels = new_arr;

    int id = bus->channel_count;
    bus->channel_count++;

    return id;
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
    if (channel < 0 || channel >= bus->channel_count ||
        bus->channels[channel] == NULL)
        return;

    struct coro_bus_channel *ch = bus->channels[channel];

    bus->channels[channel] = NULL;

    while (!rlist_empty(&ch->send_queue.coros))
        wakeup_queue_wakeup_first(&ch->send_queue);

    while (!rlist_empty(&ch->recv_queue.coros))
        wakeup_queue_wakeup_first(&ch->recv_queue);

    coro_yield();

    delete ch;
}

int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{
    if (channel < 0 || channel >= bus->channel_count ||
        bus->channels[channel] == NULL) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    struct coro_bus_channel *ch = bus->channels[channel];

    if (ch->data.size() >= ch->size_limit) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }

    ch->data.push_back(data);

    wakeup_queue_wakeup_first(&ch->recv_queue);

    return 0;
}

int
coro_bus_send(struct coro_bus *bus, int channel, unsigned value)
{
    while (true) {
        if (channel < 0 || channel >= bus->channel_count ||
            bus->channels[channel] == NULL) {
            coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
            return -1;
        }

        if (coro_bus_try_send(bus, channel, value) == 0)
            return 0;

        if (coro_bus_errno() != CORO_BUS_ERR_WOULD_BLOCK)
            return -1;

        struct coro_bus_channel *ch = bus->channels[channel];

        wakeup_queue_suspend_this(&ch->send_queue);
    }
}

int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
    if (channel < 0 || channel >= bus->channel_count ||
        bus->channels[channel] == NULL) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    struct coro_bus_channel *ch = bus->channels[channel];

    if (ch->data.empty()) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }

    *data = ch->data.front();
    ch->data.pop_front();

    wakeup_queue_wakeup_first(&ch->send_queue);

    return 0;
}

int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
    while (true) {

        if (coro_bus_try_recv(bus, channel, data) == 0)
            return 0;

        if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL)
            return -1;

        struct coro_bus_channel *ch = bus->channels[channel];

        wakeup_queue_suspend_this(&ch->recv_queue);
    }
}

int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data)
{
    bool has_channel = false;

    for (int i = 0; i < bus->channel_count; i++) {
        struct coro_bus_channel *ch = bus->channels[i];
        if (ch == NULL)
            continue;

        has_channel = true;

        if (ch->data.size() >= ch->size_limit) {
            coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
            return -1;
        }
    }

    if (!has_channel) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    for (int i = 0; i < bus->channel_count; i++) {
        struct coro_bus_channel *ch = bus->channels[i];
        if (ch == NULL)
            continue;

        ch->data.push_back(data);
        wakeup_queue_wakeup_first(&ch->recv_queue);
    }

    return 0;
}

int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
    while (true) {

        if (coro_bus_try_broadcast(bus, data) == 0)
            return 0;

        if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL)
            return -1;

        for (int i = 0; i < bus->channel_count; i++) {
            struct coro_bus_channel *ch = bus->channels[i];
            if (ch == NULL)
                continue;

            if (ch->data.size() >= ch->size_limit)
                wakeup_queue_suspend_this(&ch->send_queue);
        }
    }
}

int
coro_bus_try_send_v(struct coro_bus *bus, int channel,
    const unsigned *data, unsigned count)
{
    if (channel < 0 || channel >= bus->channel_count ||
        bus->channels[channel] == NULL) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    struct coro_bus_channel *ch = bus->channels[channel];

    if (ch->data.size() >= ch->size_limit) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }

    unsigned sent = 0;

    while (sent < count && ch->data.size() < ch->size_limit) {
        ch->data.push_back(data[sent]);
        sent++;
    }

    if (sent > 0)
        wakeup_queue_wakeup_first(&ch->recv_queue);

    return sent;
}

int
coro_bus_send_v(struct coro_bus *bus, int channel,
    const unsigned *data, unsigned count)
{
    unsigned sent_total = 0;

    while (sent_total < count) {

        int res = coro_bus_try_send_v(
            bus, channel, data + sent_total, count - sent_total);

        if (res > 0) {
            sent_total += res;
            return sent_total;
        }

        if (res == -1 && coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL)
            return -1;

        struct coro_bus_channel *ch = bus->channels[channel];
        wakeup_queue_suspend_this(&ch->send_queue);
    }

    return sent_total;
}

int
coro_bus_try_recv_v(struct coro_bus *bus, int channel,
    unsigned *data, unsigned capacity)
{
    if (channel < 0 || channel >= bus->channel_count ||
        bus->channels[channel] == NULL) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    struct coro_bus_channel *ch = bus->channels[channel];

    if (ch->data.empty()) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }

    unsigned received = 0;

    while (received < capacity && !ch->data.empty()) {
        data[received] = ch->data.front();
        ch->data.pop_front();
        received++;
    }

    wakeup_queue_wakeup_first(&ch->send_queue);

    return received;
}

int
coro_bus_recv_v(struct coro_bus *bus, int channel,
    unsigned *data, unsigned capacity)
{
    while (true) {

        int res = coro_bus_try_recv_v(bus, channel, data, capacity);

        if (res > 0)
            return res;

        if (res == -1 && coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL)
            return -1;

        struct coro_bus_channel *ch = bus->channels[channel];
        wakeup_queue_suspend_this(&ch->recv_queue);
    }
}