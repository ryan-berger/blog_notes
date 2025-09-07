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

int main(int argc, char *argv[]) {
    char* filename = argv[1];
    size_t size_bytes = strtoull(argv[2], NULL, 10);
    size_t total_ints = size_bytes / sizeof(int);
    size_t count = 0;

    int fd = open(filename, O_RDONLY|O_DIRECT);
    void *buf;
    posix_memalign(&buf, 4096, size_bytes);

    uint32_t *data = __builtin_assume_aligned(buf, 32);

    size_t off = 0;
    while (off < size_bytes) {
        ssize_t n = read(fd, (char*)data + off, size_bytes - off);
        off += (size_t)n;   // YOLO: assume n > 0 until done
    }

    long start = get_time_us();
    
    __builtin_assume(total_ints % 8 == 0);

    #pragma omp parallel for simd schedule(static) reduction(+:count) num_threads(16)
    for (size_t i = 0; i < total_ints; ++i) {
        if (data[i] == 10) count++;
    }
    long elapsed = get_time_us() - start;

    printf("simple loop %ld 10s processed at %0.2f GB/s\n",
           count,
           (double)(size_bytes/1073741824)/((double)elapsed/1.0e6));

}
