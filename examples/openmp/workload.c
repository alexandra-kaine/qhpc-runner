#define _POSIX_C_SOURCE 200809L
#include <math.h>
#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static long long parse_steps(int argc, char **argv) {
    long long steps = 200000000LL;
    for (int i = 1; i + 1 < argc; ++i) {
        if (strcmp(argv[i], "--steps") == 0) {
            steps = atoll(argv[i + 1]);
        }
    }
    return steps;
}

int main(int argc, char **argv) {
    long long steps = parse_steps(argc, argv);
    const double dx = 1.0 / (double)steps;
    double sum = 0.0;

    double t0 = omp_get_wtime();

    #pragma omp parallel for reduction(+:sum) schedule(static)
    for (long long i = 0; i < steps; ++i) {
        double x = (i + 0.5) * dx;
        /* Compute-heavy smooth function with a known finite integral. */
        sum += sin(x) * exp(-0.25 * x) + sqrt(x + 1.0);
    }

    double elapsed = omp_get_wtime() - t0;
    double result = sum * dx;

    printf(
        "QHPC_RESULT threads=%d steps=%lld elapsed_sec=%.6f value=%.12f\n",
        omp_get_max_threads(), steps, elapsed, result
    );
    return 0;
}
