# OpenMP physical-core scaling

Measured on Queen's University Frontenac HPC cluster using one OpenMP thread per physical core (`--hint=nomultithread`).

12 completed runs, with three repeats per thread count. Results below use the median elapsed time.

| threads | runs | median elapsed (s) | spread | speedup | efficiency |
|---|---:|---:|---:|---:|---:|
| 1 | 3 | 2.072102 | 1.1% | 1.00x | 100% |
| 2 | 3 | 1.137157 | 1.1% | 1.82x | 91% |
| 4 | 3 | 0.604352 | 1.4% | 3.43x | 86% |
| 8 | 3 | 0.296833 | 0.1% | 6.98x | 87% |

## CPU-affinity validation

An earlier 8-thread run achieved 4.2x speedup. Runtime affinity inspection showed that Slurm had allocated eight logical CPUs across only four physical cores using simultaneous multithreading (SMT).

The experiment was repeated with `--hint=nomultithread`, giving one OpenMP thread per physical core. The resulting 8-core run achieved 6.98x speedup with 87% parallel efficiency.

This distinction was verified using Slurm CPU allocation metadata, `lscpu`, and OpenMP affinity diagnostics.
