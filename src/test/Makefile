#
# tiny Makefile for hio tests
#

.PHONY: all clean

.SECONDARY:

all: test01.x xexec2.x

clean:
	rm -f *.x *.o

CC := mpicc

HIO_BASE := ..

CFLAGS := -g -Wall -Werror -std=c99 -I ${HIO_BASE}/include -DMPI -DHIO

LDFLAGS := -g -L${HIO_BASE}/.libs -lhio 

%.x : %.o
	$(CC) $(LDFLAGS) $^ $(LOADLIBES) $(LDLIBS) -o $@

# --- end of Makefile ---
