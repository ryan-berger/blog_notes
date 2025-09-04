#include <liburing.h>
#include <sys/time.h>

// Buffer state constants
enum buffer_state {
    BUFFER_UNUSED = -1,        // Empty buffer
    BUFFER_PREFETCHING = -2,   // Outstanding IO will fill this buffer, don't touch
    BUFFER_PREFETCHED = -3,    // Buffer is prefetched
};

typedef struct {
    // Disk I/O fields
    void* buffer;           // Single large buffer allocation for all I/O buffers
    void** io_buffers;      // Pool of aligned I/O buffers (pointers into buffer)
    size_t buffer_stride;   // Size of each I/O buffer in bytes
    size_t file_size;
    uint64_t uids;
    struct worker_thread_data* workers; // Array of worker data
    int* buffer_state;      // Maps I/O buffer index to extent index (-1 if unused)
    int num_max_bgio;
    int block_size;
    int disk_fd;            // File descriptor for disk storage
    int num_io_buffers;     // Number of I/O buffers in pool
    int num_workers;        // Number of worker threads
} ioengine_t;

// Worker thread data structure for multithreaded I/O
typedef struct worker_thread_data {
    // Worker identification
    int worker_id;
    int num_workers;
    // Pointer back to main diskarray structure
    void* na_ptr;
    // Independent I/O infrastructure
    struct io_uring ring;       // Dedicated io_uring instance
    int num_io_buffers;         // Number of buffers assigned to this worker
    int buffer_start_idx;       // First buffer index in main array
    int buffer_tail;            // To read pos
    int buffer_head;            // Last queued
    int* complete_ring;
    // Independent background I/O tracking
    int num_max_bgio;           // Maximum outstanding operations
    int bgio_head;              // Head position in ring buffers
    int bgio_tail;              // Tail position in ring buffers
    uint64_t* bgio_uids;        // Ring buffer tracking I/O unique IDs
    int* bgio_buffer_idx;       // Ring buffer tracking buffer indices
    long* bgio_block_idx;       // Ring buffer tracking block indices
    int* bgio_queued;           // Ring buffer tracking queued state: -1=requested, 1=queued, 0=completed
    // Submission signaling (lock-free)
    volatile uint64_t bgio_submit;      // Main thread increments to signal
    uint64_t bgio_submit_cached;        // Worker's cached copy for change detection
    // Submission batching control
    int pending_ios;                    // Count of queued but unsubmitted I/Os
    uint64_t last_queue_time_us;        // Timestamp of last I/O queued (microseconds)
    // Thread management
    pthread_t thread_id;
    volatile bool running;      // Thread lifecycle control
    volatile bool shutdown;     // Shutdown signal
} worker_thread_data_t;

void ioengine_free(ioengine_t* na);
ioengine_t* ioengine_alloc(const char* disk_file, size_t file_size, int num_io_buffers, int block_size, int workers);
long get_time_us(void);