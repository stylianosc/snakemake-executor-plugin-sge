# snakemake-executor-plugin-sge

A [Snakemake](https://snakemake.readthedocs.io) executor plugin for submitting
jobs to **Sun Grid Engine** (SGE), **Univa Grid Engine** (UGE), and
**Open Grid Scheduler** (OGS) clusters.

## Features

- Native `qsub` / `qstat` / `qdel` integration
- **Group jobs are submitted as SGE array jobs** (`-t 1-N`), reducing scheduler
  overhead and allowing the cluster to manage task parallelism
- Comprehensive resource mapping: CPU threads, memory (`h_vmem`), walltime
  (`h_rt`), parallel environment (`-pe`), queue (`-q`), project (`-P`)
- Optional `sge_extra` pass-through for any extra `qsub` flags
- Automatic log directory management with configurable retention
- Clean cancellation of all active jobs on interrupt

## Installation

```bash
pip install snakemake-executor-plugin-sge
```

## Usage

```bash
snakemake --executor sge --jobs 100
```

### Common options

| Flag | Description |
|---|---|
| `--sge-queue` | SGE queue to submit jobs to |
| `--sge-pe` | Parallel environment name for multi-threaded jobs |
| `--sge-project` | SGE project (`-P`) |
| `--sge-logdir` | Override default log directory |
| `--sge-keep-successful-logs` | Keep log files for successful jobs |
| `--sge-group-jobs-as-array` | Submit group jobs as array jobs (default: true) |
| `--sge-array-limit` | Max tasks per single `qsub -t` call (default: 75000) |

### Resource directives

In your `Snakefile` rules use:

```python
rule example:
    input: ...
    output: ...
    resources:
        runtime=60,          # wall time in minutes (h_rt)
        mem_mb=4096,         # memory in MB (h_vmem)
        threads=4,           # CPUs (mapped to -pe <pe> <threads>)
        sge_queue="highmem", # override queue per rule
        sge_project="myproj",# override project per rule
        sge_extra="-l gpu=1" # any extra qsub flags
    threads: 4
    shell: "..."
```

## Array-job behaviour for group jobs

When Snakemake creates a **group job** (i.e. multiple rules merged into a
single submission), this plugin packages all tasks into a single
`qsub -t 1-N` array job.  Each array task receives its command via the
`SGE_TASK_ID` environment variable, which is used to index into a compressed
task map embedded in the submission script.

This is functionally equivalent to how
[snakemake-executor-plugin-slurm](https://github.com/snakemake/snakemake-executor-plugin-slurm)
handles SLURM array jobs.

## License

MIT
