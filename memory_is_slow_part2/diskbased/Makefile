CC = gcc
BUILD_TYPE ?= release

ifeq ($(BUILD_TYPE),debug)
    CFLAGS = -O0 -g -Wall -Wextra -std=c99 -D_GNU_SOURCE  -DDEBUG_USLEEP=1
else
    CFLAGS = -O2 -Wall -Wextra -std=c99 -DNDEBUG -D_GNU_SOURCE
endif

LDFLAGS = -lm -luring
INCLUDES = -I../inc -I../../common/inc

TARGETS = benchmark

.PHONY: all clean

all: $(TARGETS)

benchmark: benchmark.c io_engine.c
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGETS)
