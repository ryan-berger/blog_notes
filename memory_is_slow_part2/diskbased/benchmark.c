#include "io_engine.h"
#include <sys/mman.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <stdint.h>
#include <sys/time.h>

#define DEFAULT_WORKERS 6
#define DEFAULT_BLOCK_SIZE 16384
#define DEFAULT_QUEUE_DEPTH 8192 // per worker

// Count the number of "10" (int format) in the buffer
static inline size_t count_tens_unrolled(void* data, size_t size_bytes) {
    const size_t total = size_bytes / sizeof(int);
    // Get the compiler to align the buffer
    const int * __restrict p = (const int * __restrict)__builtin_assume_aligned(data, 4096);
    uint64_t c0=0, c1=0, c2=0, c3=0,
            c4=0, c5=0, c6=0, c7=0,
            c8=0, c9=0, c10=0, c11=0,
            c12=0, c13=0, c14=0, c15=0;

    // Unrolling the compiler knows it can use a vector unit like AVX2 to process
    for (size_t i = 0; i < total; i += 16) {
        // removed 'if' to get it to be branchless: each compares to 10, adds 0 or 1
        c0  += (unsigned)(p[i+ 0] == 10);
        c1  += (unsigned)(p[i+ 1] == 10);
        c2  += (unsigned)(p[i+ 2] == 10);
        c3  += (unsigned)(p[i+ 3] == 10);
        c4  += (unsigned)(p[i+ 4] == 10);
        c5  += (unsigned)(p[i+ 5] == 10);
        c6  += (unsigned)(p[i+ 6] == 10);
        c7  += (unsigned)(p[i+ 7] == 10);
        c8  += (unsigned)(p[i+ 8] == 10);
        c9  += (unsigned)(p[i+ 9] == 10);
        c10 += (unsigned)(p[i+10] == 10);
        c11 += (unsigned)(p[i+11] == 10);
        c12 += (unsigned)(p[i+12] == 10);
        c13 += (unsigned)(p[i+13] == 10);
        c14 += (unsigned)(p[i+14] == 10);
        c15 += (unsigned)(p[i+15] == 10);
    }

    // pairwise reduce to help some compilers schedule better
    uint64_t s0 = c0 + c1,   s1 = c2 + c3,   s2 = c4 + c5,   s3 = c6 + c7;
    uint64_t s4 = c8 + c9,   s5 = c10 + c11, s6 = c12 + c13, s7 = c14 + c15;
    uint64_t t0 = s0 + s1,   t1 = s2 + s3,   t2 = s4 + s5,   t3 = s6 + s7;

    return (t0 + t1) + (t2 + t3);
}

int main(int argc, char *argv[]) {
    char* filename = argv[1];
    size_t size_bytes = strtoull(argv[2], NULL, 10);

    // Set up the io engine
    ioengine_t* na = ioengine_alloc(filename, size_bytes, DEFAULT_QUEUE_DEPTH, DEFAULT_BLOCK_SIZE, DEFAULT_WORKERS);

    sleep(1);

    // Use the background workers to read file directly
    size_t total_blocks = na->file_size / na->block_size;
    uint64_t uid = 1;
    size_t count = 0;

    long start = get_time_us();

    // Read all blocks
    size_t blocks_queued = 0;
    size_t blocks_read = 0;
    int buffer_queued = 0;
    while (blocks_read < total_blocks) {
        //// Queue IO phase //////
        //     Do we have more blocks to queue up?
        if (buffer_queued < na->num_io_buffers/2 && blocks_queued <= total_blocks) {
            // Calculate how many blocks on average we want our workers to queue up
            size_t free_buffers = (size_t)(na->num_io_buffers - buffer_queued - 4); // hold back a few buffers
            size_t blocks_remaining = total_blocks - blocks_queued;  // how many blocks have we not queued
            size_t blocks_to_queue = free_buffers > blocks_remaining ? blocks_remaining : free_buffers;
            int blocks_to_queue_per_worker = (int) (blocks_to_queue + na->num_workers - 1) / na->num_workers;
            // Iterate through workers and assign work
            for (int i = 0; i < na->num_workers; i++) {
                worker_thread_data_t* worker = &na->workers[i];
                // Try to queue N blocks to this worker
                for (int j = 0; j < blocks_to_queue_per_worker; j++) {
                    if (blocks_queued == total_blocks) break;
                    int bgio_tail = worker->bgio_tail;
                    int bgio_head = worker->bgio_head;
                    int bgio_next = (bgio_tail + 1) % worker->num_max_bgio;
                    int next_bhead = (worker->buffer_head + 1) % worker->num_max_bgio;
                    if (bgio_next == bgio_head) break;  // queue for send requests is full
                    if (next_bhead == worker->buffer_tail) break; // queue for recieving completed IO is full
                    // Queue this block with the worker.  We have to track which buffer it's going to.
                    int buffer_idx = worker->buffer_start_idx + worker->buffer_head;
                    na->buffer_state[buffer_idx] = BUFFER_PREFETCHING;
                    worker->bgio_uids[bgio_tail] = (uid++)<<16; // unique id helps track IOs in io_uring, we encode 4 bytes later
                    worker->bgio_buffer_idx[bgio_tail] = buffer_idx;
                    worker->bgio_block_idx[bgio_tail] = blocks_queued++;  // block sized index into file
                    worker->bgio_queued[bgio_tail] = -1;  // Requested but not yet queued
                    int next_tail = (bgio_tail + 1) % worker->num_max_bgio;
                    worker->bgio_tail = next_tail;
                    // Log the buffer in an ordered queue for us to read
                    worker->complete_ring[worker->buffer_head] = buffer_idx;
                    worker->buffer_head = next_bhead;
                    buffer_queued++;
                }
                // Tell the worker to submit IOs as a group
                worker->bgio_submit++;
            }
        }

        //// Completion Phase //////
        //     Iterate through worker and check if they have complete IOs
        for (int i = 0; i < na->num_workers; i++) {
            worker_thread_data_t* worker = &na->workers[i];
            int current = worker->buffer_tail;
            // We know what IO's we're waiting on, but we have to poll
            //  to see if they are done.
            for (int scan = 0; scan < worker->num_max_bgio; scan++) {
                // Scan until we get to the end of the list
                if (current == worker->buffer_head) break;
                int buffer_idx = worker->complete_ring[current];
                int state = na->buffer_state[buffer_idx];
                if (state == BUFFER_PREFETCHED) {
                    // This buffer is completed - Process this buffer.
                    count += count_tens_unrolled(na->io_buffers[buffer_idx], na->block_size);
                    na->buffer_state[buffer_idx] = BUFFER_UNUSED;
                    blocks_read++;
                    buffer_queued--;
                }
                current = (current + 1) % worker->num_max_bgio;
            }
            // IO's might have been completed out of order, advance the tail when we can
            current = worker->buffer_tail;
            while (current != worker->buffer_head) {
                int buffer_idx = worker->complete_ring[current];
                int state = na->buffer_state[buffer_idx];
                if (state != BUFFER_UNUSED) break;
                current = (current + 1) % worker->num_max_bgio;
            }
            worker->buffer_tail = current;
            worker->bgio_submit++;  // probably unnecessary
        }
    }
    long elapsed = get_time_us() - start;
    printf("diskbased found %ld 10s processed at %0.2f GB/s\n", count, (double)(size_bytes/1073741824)/((double)elapsed/1.0e6));

    // Cleanup I/O system
    ioengine_free(na);

    return 0;
}
