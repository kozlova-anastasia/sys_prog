#include "thread_pool.h"

#include <pthread.h>
#include <errno.h>
#include <time.h>
#include <math.h>

#include <deque>
#include <vector>

struct thread_pool;

struct thread_task {
	thread_task_f function;

	pthread_mutex_t mutex;
	pthread_cond_t cond;

	thread_pool *pool;

	bool is_queued;
	bool is_running;
	bool is_finished;
	bool is_detached;
};

struct thread_pool {
	std::vector<pthread_t> threads;
	std::deque<thread_task *> queue;

	pthread_mutex_t mutex;
	pthread_cond_t cond;

	int max_threads;
	int waiting_threads;
	int task_count;
	bool is_stopping;
};

static void
thread_task_destroy(struct thread_task *task)
{
	pthread_cond_destroy(&task->cond);
	pthread_mutex_destroy(&task->mutex);
	delete task;
}

static void *
thread_pool_worker(void *arg)
{
	thread_pool *pool = (thread_pool *)arg;

	while (true) {
		pthread_mutex_lock(&pool->mutex);

		while (pool->queue.empty() && !pool->is_stopping) {
			pool->waiting_threads++;
			pthread_cond_wait(&pool->cond, &pool->mutex);
			pool->waiting_threads--;
		}

		if (pool->is_stopping && pool->queue.empty()) {
			pthread_mutex_unlock(&pool->mutex);
			break;
		}

		thread_task *task = pool->queue.front();
		pool->queue.pop_front();

		pthread_mutex_unlock(&pool->mutex);

		pthread_mutex_lock(&task->mutex);
		task->is_queued = false;
		task->is_running = true;
		pthread_mutex_unlock(&task->mutex);

		task->function();

		bool need_delete = false;

		pthread_mutex_lock(&task->mutex);
		task->is_running = false;
		task->is_finished = true;

		if (task->is_detached) {
			task->pool = nullptr;
			need_delete = true;
		} else {
			pthread_cond_broadcast(&task->cond);
		}
		pthread_mutex_unlock(&task->mutex);

		if (need_delete) {
			pthread_mutex_lock(&pool->mutex);
			pool->task_count--;
			pthread_mutex_unlock(&pool->mutex);
			thread_task_destroy(task);
		}
	}

	return NULL;
}

int
thread_pool_new(int thread_count, struct thread_pool **pool)
{
	if (pool == nullptr || thread_count <= 0 || thread_count > TPOOL_MAX_THREADS)
		return TPOOL_ERR_INVALID_ARGUMENT;

	thread_pool *res = new thread_pool();
	res->max_threads = thread_count;
	res->waiting_threads = 0;
	res->task_count = 0;
	res->is_stopping = false;

	pthread_mutex_init(&res->mutex, NULL);
	pthread_cond_init(&res->cond, NULL);

	*pool = res;
	return 0;
}

int
thread_pool_delete(struct thread_pool *pool)
{
	if (pool == nullptr)
		return TPOOL_ERR_INVALID_ARGUMENT;

	pthread_mutex_lock(&pool->mutex);
	if (pool->task_count != 0) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_HAS_TASKS;
	}

	pool->is_stopping = true;
	pthread_cond_broadcast(&pool->cond);
	pthread_mutex_unlock(&pool->mutex);

	for (size_t i = 0; i < pool->threads.size(); ++i)
		pthread_join(pool->threads[i], NULL);

	pthread_cond_destroy(&pool->cond);
	pthread_mutex_destroy(&pool->mutex);
	delete pool;
	return 0;
}

int
thread_pool_push_task(struct thread_pool *pool, struct thread_task *task)
{
	if (pool == nullptr || task == nullptr)
		return TPOOL_ERR_INVALID_ARGUMENT;

	pthread_mutex_lock(&pool->mutex);
	if (pool->task_count >= TPOOL_MAX_TASKS) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_TOO_MANY_TASKS;
	}

	pthread_mutex_lock(&task->mutex);

	task->pool = pool;
	task->is_queued = true;
	task->is_running = false;
	task->is_finished = false;
	task->is_detached = false;

	pool->queue.push_back(task);
	pool->task_count++;

	if ((int)pool->threads.size() < pool->max_threads &&
	    (int)pool->queue.size() > pool->waiting_threads) {
		pthread_t tid;
		pthread_create(&tid, NULL, thread_pool_worker, pool);
		pool->threads.push_back(tid);
	}

	pthread_cond_signal(&pool->cond);

	pthread_mutex_unlock(&task->mutex);
	pthread_mutex_unlock(&pool->mutex);
	return 0;
}

int
thread_task_new(struct thread_task **task, const thread_task_f &function)
{
	if (task == nullptr)
		return TPOOL_ERR_INVALID_ARGUMENT;

	thread_task *res = new thread_task();
	res->function = function;
	res->pool = nullptr;
	res->is_queued = false;
	res->is_running = false;
	res->is_finished = false;
	res->is_detached = false;

	pthread_mutex_init(&res->mutex, NULL);
	pthread_cond_init(&res->cond, NULL);

	*task = res;
	return 0;
}

bool
thread_task_is_finished(const struct thread_task *task)
{
	if (task == nullptr)
		return false;

	pthread_mutex_lock((pthread_mutex_t *)&task->mutex);
	bool result = task->is_finished;
	pthread_mutex_unlock((pthread_mutex_t *)&task->mutex);
	return result;
}

bool
thread_task_is_running(const struct thread_task *task)
{
	if (task == nullptr)
		return false;

	pthread_mutex_lock((pthread_mutex_t *)&task->mutex);
	bool result = task->is_running;
	pthread_mutex_unlock((pthread_mutex_t *)&task->mutex);
	return result;
}

int
thread_task_join(struct thread_task *task)
{
	if (task == nullptr)
		return TPOOL_ERR_INVALID_ARGUMENT;

	pthread_mutex_lock(&task->mutex);

	if (task->pool == nullptr) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}

	while (!task->is_finished)
		pthread_cond_wait(&task->cond, &task->mutex);

	thread_pool *pool = task->pool;
	task->pool = nullptr;
	task->is_queued = false;
	task->is_running = false;

	pthread_mutex_unlock(&task->mutex);

	pthread_mutex_lock(&pool->mutex);
	pool->task_count--;
	pthread_mutex_unlock(&pool->mutex);

	return 0;
}

#if NEED_TIMED_JOIN

static void
make_abs_timespec(struct timespec *ts, double timeout)
{
	clock_gettime(CLOCK_REALTIME, ts);

	if (timeout <= 0)
		return;

	time_t sec = (time_t)timeout;
	double frac = timeout - (double)sec;
	long nsec = (long)(frac * 1000000000.0);

	ts->tv_sec += sec;
	ts->tv_nsec += nsec;

	if (ts->tv_nsec >= 1000000000L) {
		ts->tv_sec += 1;
		ts->tv_nsec -= 1000000000L;
	}
}

int
thread_task_timed_join(struct thread_task *task, double timeout)
{
	if (task == nullptr)
		return TPOOL_ERR_INVALID_ARGUMENT;

	pthread_mutex_lock(&task->mutex);

	if (task->pool == nullptr) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}

	if (!task->is_finished) {
		if (timeout <= 0) {
			pthread_mutex_unlock(&task->mutex);
			return TPOOL_ERR_TIMEOUT;
		}

		if (isinf(timeout)) {
			while (!task->is_finished)
				pthread_cond_wait(&task->cond, &task->mutex);
		} else {
			struct timespec ts;
			make_abs_timespec(&ts, timeout);

			while (!task->is_finished) {
				int rc = pthread_cond_timedwait(&task->cond, &task->mutex, &ts);
				if (rc == ETIMEDOUT && !task->is_finished) {
					pthread_mutex_unlock(&task->mutex);
					return TPOOL_ERR_TIMEOUT;
				}
			}
		}
	}

	thread_pool *pool = task->pool;
	task->pool = nullptr;
	task->is_queued = false;
	task->is_running = false;

	pthread_mutex_unlock(&task->mutex);

	pthread_mutex_lock(&pool->mutex);
	pool->task_count--;
	pthread_mutex_unlock(&pool->mutex);

	return 0;
}

#endif

int
thread_task_delete(struct thread_task *task)
{
	if (task == nullptr)
		return TPOOL_ERR_INVALID_ARGUMENT;

	pthread_mutex_lock(&task->mutex);
	bool in_pool = (task->pool != nullptr);
	pthread_mutex_unlock(&task->mutex);

	if (in_pool)
		return TPOOL_ERR_TASK_IN_POOL;

	thread_task_destroy(task);
	return 0;
}

#if NEED_DETACH

int
thread_task_detach(struct thread_task *task)
{
	if (task == nullptr)
		return TPOOL_ERR_INVALID_ARGUMENT;

	pthread_mutex_lock(&task->mutex);

	if (task->pool == nullptr) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}

	thread_pool *pool = task->pool;

	if (task->is_finished) {
		task->pool = nullptr;
		task->is_detached = true;
		pthread_mutex_unlock(&task->mutex);

		pthread_mutex_lock(&pool->mutex);
		pool->task_count--;
		pthread_mutex_unlock(&pool->mutex);

		thread_task_destroy(task);
		return 0;
	}

	task->is_detached = true;
	pthread_mutex_unlock(&task->mutex);
	return 0;
}

#endif
