#include "io_engine.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

long get_time_us() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000L + tv.tv_usec;
}

static inline void ioengine_worker_process_completions(ioengine_t* na, worker_thread_data_t* worker) {
    struct io_uring_cqe *cqe;
    unsigned head;
    unsigned processed = 0;
    int bgio_tail = worker->bgio_tail;
    int bgio_head = worker->bgio_head;
    io_uring_for_each_cqe(&worker->ring, head, cqe) {
        uint64_t io_uid = (int)io_uring_cqe_get_data64(cqe);
        int slot = (int)(io_uid & 0xFFFF);

        if (slot < 0) {
            printf("ERROR: Worker %d unmatched completion UID=%d\n", worker->worker_id, slot);
            exit(1);
        }

        // Handle the matched completion
        int buffer_idx = worker->bgio_buffer_idx[slot];
        long block_idx = worker->bgio_block_idx[slot];
        int state = na->buffer_state[buffer_idx];

        // We should be in this state
        if (state != BUFFER_PREFETCHING) {
            printf("ERROR: Worker %d completion for I/O %d/%lx buffer %d in invalid state %d block %ld\n", 
                   worker->worker_id, slot, io_uid, buffer_idx, state, block_idx);
            exit(1);
        }

        // Result check
        if (cqe->res < 0 || cqe->res != (__s32)na->block_size) {
            printf("ERROR: Worker %d I/O %d failed buffer %d block %ld: %d\n", 
                   worker->worker_id, slot, buffer_idx, block_idx, cqe->res);
            exit(1);
        }

        // Clear worker's slot
        worker->bgio_uids[slot] = 0;
        worker->bgio_buffer_idx[slot] = -1;
        worker->bgio_block_idx[slot] = -1;
        worker->bgio_queued[slot] = 0;

        // Update buffer state based on operation type
        if (state == BUFFER_PREFETCHING) {
            na->buffer_state[buffer_idx] = BUFFER_PREFETCHED;
        }

        processed++;
    }

    if (processed > 0) {
        io_uring_cq_advance(&worker->ring, processed);

        // Advance worker's bgio_head past completed operations
        while (bgio_head != bgio_tail && worker->bgio_uids[bgio_head] == 0) {
            bgio_head = (bgio_head + 1) % worker->num_max_bgio;
            worker->bgio_head = bgio_head;
        }
        //printf("Worker %d bgio_head=%d bgio_tail=%d\n", worker->worker_id, worker->bgio_head, worker->bgio_tail);
    }
}

// Queue I/O operations from worker's pending queue to io_uring
static inline void ioengine_worker_queue_io(ioengine_t* na, worker_thread_data_t* worker) {
    int bgio_tail = worker->bgio_tail;
    int bgio_head = worker->bgio_head;
    while (bgio_head != bgio_tail) {
        int slot = bgio_head;
        if (slot >= worker->num_io_buffers) {
            printf("slot too big\n");
            exit(1);
        }
        uint64_t io_uid = worker->bgio_uids[slot];
        int buffer_idx = worker->bgio_buffer_idx[slot];
        long block_idx = worker->bgio_block_idx[slot];
        int iovec_idx = buffer_idx - worker->worker_id * worker->num_io_buffers;
        int queued_state = worker->bgio_queued[slot];

        // Skip already processed entries or already queued entries
        if (io_uid == 0 || queued_state != -1) {
            bgio_head = (bgio_head + 1) % worker->num_max_bgio;
            continue;
        }

        off_t offset = block_idx * na->block_size;

        // Prep I/O operation on worker's ring
        struct io_uring_sqe *sqe = io_uring_get_sqe(&worker->ring);
        if (!sqe) {
            // Ring is full, submit what we have and try again later
            break;
        }
        io_uid |= (uint16_t)slot;
        io_uring_prep_read_fixed(sqe, na->disk_fd, na->io_buffers[buffer_idx], 
                                    na->block_size, offset, iovec_idx);
        io_uring_sqe_set_data64(sqe, io_uid);

        // Mark as queued to prevent duplicate submissions
        worker->bgio_queued[slot] = 1;

        worker->pending_ios++;

        bgio_head = (bgio_head + 1) % worker->num_max_bgio;
        //printf("Worker %d queued I/O %d/%lx buffer %d block_idx %ld  offset %ld head %d\n", worker->worker_id, slot, io_uid, buffer_idx, block_idx, offset, bgio_head);
    }
}

// High-performance worker thread main loop
static void* ioengine_worker_thread_main(void* arg) {
    worker_thread_data_t* worker = (worker_thread_data_t*)arg;
    ioengine_t* na = (ioengine_t*)worker->na_ptr;

    // Initialize worker's io_uring instance
    int ret = io_uring_queue_init(worker->num_io_buffers, &worker->ring, IORING_SETUP_SQPOLL);
    if (ret < 0) {
        printf("ERROR: Worker %d io_uring_queue_init failed: %s\n", worker->worker_id, strerror(-ret));
        return NULL;
    }

    // Register buffers with worker's io_uring (all buffers for now)
    struct iovec *iovecs = malloc(na->num_io_buffers * sizeof(struct iovec));
    if (!iovecs) {
        printf("ERROR: Worker %d failed to allocate iovecs\n", worker->worker_id);
        io_uring_queue_exit(&worker->ring);
        return NULL;
    }

    int start_idx = worker->num_io_buffers*worker->worker_id;
    for (int j = 0; j < worker->num_io_buffers; j++) {
        iovecs[j].iov_base = na->io_buffers[start_idx + j];
        iovecs[j].iov_len = na->block_size;
    }
    ret = io_uring_register_buffers(&worker->ring, iovecs, worker->num_io_buffers);
    free(iovecs);
    if (ret < 0) {
        printf("ERROR: Worker %d io_uring_register_buffers failed: %s\n", worker->worker_id, strerror(-ret));
        io_uring_queue_exit(&worker->ring);
        return NULL;
    }
    worker->running = true;

    int loopcnt = 0;
    while (!worker->shutdown) {
        //printf("Worker %d loopcnt %d\n", worker->worker_id, loopcnt);
        bool should_submit = false;

        // Check for new work signal (lock-free)
        if (worker->bgio_submit != worker->bgio_submit_cached) {
            //printf("INFO: New work signal received\n");
            should_submit = true;
            worker->bgio_submit_cached = worker->bgio_submit;
        }

        // Process queued work (prep I/O operations)
        ioengine_worker_queue_io(na, worker);

        if (worker->pending_ios >= 128) {
            should_submit = true;  // Batch size trigger
        }

        if (loopcnt >= 4000) {
            should_submit = true;  // Timeout trigger (60μs @ 3GHz)
            loopcnt = 0;
        }

        // Submit when triggered
        if (should_submit && worker->pending_ios > 0) {
            int submitted = io_uring_submit(&worker->ring);
            if (submitted < 0) {
                printf("ERROR: Worker %d io_uring_submit failed: %s\n", 
                       worker->worker_id, strerror(-submitted));
                exit(1);
            }
            worker->pending_ios = 0;  // Reset pending counter
        }

        // Process completions
        ioengine_worker_process_completions(na, worker);

        #ifdef DEBUG_USLEEP
        usleep(10); // DEBUG: Sleep for a bit, otherwise valgrind can't keep up
        #endif
        loopcnt++;
    }

    // Cleanup worker's io_uring on exit
    io_uring_queue_exit(&worker->ring);

    worker->running = false;
    return NULL;
}

// Cleanup worker threads
static void ioengine_cleanup_workers(ioengine_t* na) {
    if (!na->workers) return;

    // Signal all workers to shutdown
    for (int i = 0; i < na->num_workers; i++) {
        na->workers[i].shutdown = true;
    }

    // Wait for all workers to finish
    for (int i = 0; i < na->num_workers; i++) {
        worker_thread_data_t* worker = &na->workers[i];
        if (worker->thread_id) {
            pthread_join(worker->thread_id, NULL);
        }

        // Cleanup worker resources (io_uring cleaned up by worker thread)
        free(worker->bgio_uids);
        free(worker->bgio_buffer_idx);
        free(worker->bgio_block_idx);
        free(worker->bgio_queued);
    }
    free(na->workers);
    na->workers = NULL;
    na->num_workers = 0;

    //printf("  INFO: Cleaned up worker threads\n");
}

// Initialize worker threads for multithreaded I/O
static int ioengine_init_workers(ioengine_t* na, int num_workers) {
    if (num_workers <= 0) {
        printf("ERROR: Invalid number of worker threads %d\n", num_workers);
        return -1;
    }

    na->num_workers = num_workers;

    // Allocate worker array
    na->workers = calloc(num_workers, sizeof(worker_thread_data_t));
    if (!na->workers) {
        printf("ERROR: Failed to allocate worker array\n");
        return -1;
    }

    for (int i = 0; i < num_workers; i++) {
        worker_thread_data_t* worker = &na->workers[i];

        // Worker identification
        worker->worker_id = i;
        worker->num_workers = num_workers;
        worker->num_io_buffers = na->num_io_buffers / num_workers;
        worker->buffer_start_idx = i * worker->num_io_buffers;
        worker->na_ptr = na;

        // Initialize worker's background I/O tracking
        worker->num_max_bgio = na->num_max_bgio/worker->num_workers;
        worker->bgio_head = 0;
        worker->bgio_tail = 0;
        worker->buffer_head = 0;
        worker->buffer_tail = 0;

        uint64_t bgio_uids_size = ((worker->num_max_bgio * sizeof(uint64_t) + 511) / 512) * 512;
        uint64_t bgio_buffer_idx_size = ((worker->num_max_bgio * sizeof(int) + 511) / 512) * 512;
        uint64_t bgio_block_idx_size = ((worker->num_max_bgio * sizeof(long) + 511) / 512) * 512;
        uint64_t bgio_queued_size = ((worker->num_max_bgio * sizeof(int) + 511) / 512) * 512;

        worker->bgio_uids = aligned_alloc(512, bgio_uids_size);
        worker->bgio_buffer_idx = aligned_alloc(512, bgio_buffer_idx_size);
        worker->complete_ring = aligned_alloc(512, bgio_buffer_idx_size);
        worker->bgio_block_idx = aligned_alloc(512, bgio_block_idx_size);
        worker->bgio_queued = aligned_alloc(512, bgio_queued_size);

        if (!worker->bgio_uids || !worker->bgio_buffer_idx || 
            !worker->bgio_block_idx || !worker->bgio_queued) {
            printf("ERROR: Worker %d failed to allocate bgio arrays\n", i);
            return -1;
        }

        // Initialize submission control
        worker->bgio_submit = 0;
        worker->bgio_submit_cached = 0;
        worker->pending_ios = 0;

        // Initialize thread control
        worker->running = false;
        worker->shutdown = false;

        // Create worker thread
        int pthread_ret = pthread_create(&worker->thread_id, NULL, 
                                        ioengine_worker_thread_main, worker);
        if (pthread_ret != 0) {
            printf("ERROR: Failed to create worker thread %d: %s\n", i, strerror(pthread_ret));
            return -1;
        }
    }

    //printf("  INFO: Initialized %d worker threads\n", num_workers);
    return 0;
}

/// PUBLIC /////////////////////////////////////////////

void ioengine_free(ioengine_t* na) {
    ioengine_cleanup_workers(na);
    free(na->buffer);
    free(na->io_buffers);
    free(na->buffer_state);
    free(na);
}


// Create an empty diskarray with disk backing
ioengine_t* ioengine_alloc(const char* disk_file, size_t file_size, int num_io_buffers, int block_size, int workers) {
    ioengine_t* na = calloc(1, sizeof(ioengine_t));
    if (!na) {
        printf("ERROR: Failed to allocate ioengine_t\n");
        return NULL;
    }
    na->num_io_buffers = num_io_buffers;
    na->num_workers = workers;
    na->file_size = file_size;
    na->block_size = block_size;

    na->num_max_bgio = na->num_io_buffers/2;

    // Open or create disk file
    na->disk_fd = open(disk_file, O_CREAT | O_RDWR | O_DIRECT, 0644);
    if (na->disk_fd < 0) {
        printf("Failed to open disk file %s: %s\n", disk_file, strerror(errno));
        free(na);
        return NULL;
    }

    // Initialize extent file size
    if (ftruncate(na->disk_fd, file_size) < 0) {
        printf("Failed to set disk file size: %s, You'd better be using a raw disk.\n", strerror(errno));
        // close(na->disk_fd);
        // free(na);
        // return NULL;
    }

    // Calculate size for single large buffer allocation
    // Each buffer needs extent_disk_size rounded up to 4K for alignment
    na->buffer_stride = ((na->block_size + 4095) / 4096) * 4096;
    size_t total_buffer_size = na->num_io_buffers * na->buffer_stride;
    
    // Single large aligned allocation for all I/O buffers
    if (posix_memalign(&na->buffer, 4096, total_buffer_size) != 0) {
        printf("Failed to allocate large I/O buffer (%zu bytes)\n", total_buffer_size);
        return NULL;
    }
    memset(na->buffer, 0, total_buffer_size);

    uint64_t io_buffers_size = ((na->num_io_buffers * sizeof(void*) + 4095) / 4096) * 4096;
    na->io_buffers = aligned_alloc(4096, io_buffers_size);

    uint64_t buffer_state_size = ((na->num_io_buffers * sizeof(int) + 511) / 512) * 512;
    na->buffer_state = aligned_alloc(512, buffer_state_size);

    // Set up io_buffers pointers into the large buffer
    for (int i = 0; i < na->num_io_buffers; i++) {
        na->buffer_state[i] = BUFFER_UNUSED; // Unused
        na->io_buffers[i] = (char*)na->buffer + (i * na->buffer_stride);
    }

    if (ioengine_init_workers(na, workers) != 0) {
        printf("ERROR: Failed to initialize I/O system\n");
        return NULL;
    }

    return na;
}
