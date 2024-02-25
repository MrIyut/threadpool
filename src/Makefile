BUILD_DIR := build
UTILS_PATH ?= ../utils
CPPFLAGS := -I$(UTILS_PATH)
CFLAGS := -Wall -Wextra
# Remove the line below to disable debugging support.
CFLAGS += -g -O0
PARALLEL_LDLIBS := -lpthread

TEST_SRC:= test.c os_graph.c os_threadpool.c $(UTILS_PATH)/log/log.c
TEST_OBJS := $(patsubst %.c,%.o,$(TEST_SRC))

.PHONY: all pack clean always

all: serial parallel

parallel: $(TEST_OBJS)
	$(CC) -o $@ $^ $(PARALLEL_LDLIBS)

$(UTILS_PATH)/log/log.o: $(UTILS_PATH)/log/log.c $(UTILS_PATH)/log/log.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ $<

pack: clean
	-rm -f ../src.zip
	zip -r ../src.zip *

clean:
	-rm -f $(TEST_OBJS)
	-rm -f serial parallel
	-rm -f *~
