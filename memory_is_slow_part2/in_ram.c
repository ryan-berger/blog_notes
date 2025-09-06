#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>

long get_time_us() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000L + tv.tv_usec;
}


typedef struct {
    const int* data;
    size_t n;
    uint64_t* res;
} thread_data;

void count_tens(const thread_data* td) {
    uint64_t count = 0;
    const int* data = __builtin_assume_aligned(td->data, 64);

    size_t n = td->n;
    __builtin_assume(n % 8 == 0);
    
    #pragma unroll
    for (size_t i = 0; i < n; ++i) {
        if (data[i] == 10) count++;
    }
    
    *td->res = count;
}

#define THREADED 1
#define THREADS 4


int main(int argc, char *argv[]) {
    char* filename = argv[1];
    size_t size_bytes = strtoull(argv[2], NULL, 10);
    size_t total_ints = size_bytes / sizeof(int);
    size_t count = 0;

    int fd = open(filename, O_RDONLY|O_DIRECT);
    void *buf;
    posix_memalign(&buf, 4096, size_bytes);

    int *data = (int*)buf;

    size_t off = 0;
    while (off < size_bytes) {
        ssize_t n = read(fd, (char*)data + off, size_bytes - off);
        off += (size_t)n;   // YOLO: assume n > 0 until done
    }

    if (THREADED) {
        pthread_t threads[THREADS];
        thread_data* args = malloc(sizeof(thread_data)*THREADS);
        uint64_t* results = malloc(sizeof(uint64_t)*THREADS);
        
        size_t per_thread = total_ints / THREADS;
        long start = get_time_us();
        
        for (size_t i = 0; i < THREADS; ++i) {
            args[i] = (thread_data){ .data = &data[i*per_thread], .n = per_thread, .res = &results[i] };
            pthread_create(
                &threads[i], NULL,
                (void*)count_tens, &args[i]);
        }

        uint64_t result = 0;

        for (size_t i = 0; i < THREADS; ++i) {
            pthread_join(threads[i], NULL);
            result += results[i];
        }
        
        long elapsed = get_time_us() - start;
        printf("simple loop %ld 10s processed at %0.2f GB/s\n",
            count,
            (double)(size_bytes/1000000000)/((double)elapsed/1.0e6));

    } else {
        long start = get_time_us();
        __builtin_assume(total_ints % 8 == 0);
        for (size_t i = 0; i < total_ints; ++i) {
            if (data[i] == 10) count++;
        }
        long elapsed = get_time_us() - start;

        printf("simple loop %ld 10s processed at %0.2f GB/s\n",
               count,
               (double)(size_bytes/1073741824)/((double)elapsed/1.0e6));
    }

}
