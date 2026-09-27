CC ?= gcc
# No -march=native: we compile on the login node, but jobs run on compute
# nodes that may have a different CPU. A binary tuned for the login node's CPU
# can die with "Illegal instruction" there.
CFLAGS ?= -O3 -fopenmp
LDLIBS ?= -lm

all: ../../bin/openmp_integrate

../../bin/openmp_integrate: workload.c
	mkdir -p ../../bin
	$(CC) $(CFLAGS) $< -o $@ $(LDLIBS)

clean:
	rm -f ../../bin/openmp_integrate
