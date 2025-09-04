#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>

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
    int *data = buf;

    size_t off = 0;
    while (off < size_bytes) {
        ssize_t n = read(fd, (char*)data + off, size_bytes - off);
        off += (size_t)n;   // YOLO: assume n > 0 until done
    }

    long start = get_time_us();
    for (size_t i = 0; i < total_ints; ++i) {
        if (data[i] == 10) count++;
    }
    long elapsed = get_time_us() - start;

    printf("simple loop %ld 10s processed at %0.2f GB/s\n",
           count,
           (double)(size_bytes/1073741824)/((double)elapsed/1.0e6));


    // Get the compiler to align the buffer
    const int * __restrict p = (const int * __restrict)__builtin_assume_aligned((void*)data, 4096);
    uint64_t c0=0, c1=0, c2=0, c3=0,
            c4=0, c5=0, c6=0, c7=0,
            c8=0, c9=0, c10=0, c11=0,
            c12=0, c13=0, c14=0, c15=0;

    start = get_time_us();
    // Unrolling the compiler knows it can use a vector unit like AVX2 to process
    for (size_t i = 0; i < total_ints; i += 16) {
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

    count = (t0 + t1) + (t2 + t3);
    elapsed = get_time_us() - start;

    printf("unrolled loop %ld 10s processed at %0.2f GB/s\n",
           count,
           (double)(size_bytes/1073741824)/((double)elapsed/1.0e6));


}
