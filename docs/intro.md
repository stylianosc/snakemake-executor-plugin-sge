# Snakemake Executor Plugin for SGE/UGE/OGS

A Snakemake executor plugin for submitting jobs to Sun Grid Engine (SGE), Univa Grid Engine (UGE), and Open Grid Scheduler (OGS) clusters.

## Installation

Install the plugin using conda or pip:

```bash
# Using conda
conda install -c bioconda snakemake-executor-plugin-sge

# Using pip
pip install snakemake-executor-plugin-sge
```

## Basic Usage

To use the SGE executor, specify it when running Snakemake:

```bash
snakemake --executor sge --jobs 100
```

This will submit each Snakemake job as a separate SGE job (`qsub`), allowing up to 100 concurrent jobs.

### Key Features

- **Direct cluster submission**: Jobs are submitted to SGE/UGE/OGS via `qsub`
- **Array jobs**: Group jobs are automatically submitted as SGE array jobs (`qsub -t 1-N`) to reduce scheduler overhead
- **Resource specification**: Define per-rule resource requirements (memory, runtime, threads, queue)
- **Cross-job dependencies**: Snakemake DAG dependencies are translated to SGE dependencies
- **Automatic log management**: Job logs are collected and optionally cleaned up after workflow completion

## Configuration

### Command-line Flags

Common flags to configure the executor:

```bash
# Specify SGE queue
snakemake --executor sge --sge-queue high.q --jobs 100

# Set default memory per job
snakemake --executor sge --default-resources mem_mb=8000 --jobs 100

# Disable array jobs for group jobs
snakemake --executor sge --sge-disable-group-jobs-as-array --jobs 100
```

### Per-Rule Resources

Define resource requirements in your Snakemake rules:

```python
rule my_analysis:
    input: "input.txt"
    output: "output.txt"
    resources:
        mem_mb=16000,      # Memory in MB
        runtime=120,       # Runtime in minutes
        threads=8,         # Number of threads
        sge_queue="high.q", # SGE queue (optional)
        sge_project="myproject", # Project code (optional)
    shell:
        "process_data.sh {input} {output}"
```

### Environment Variables

The executor respects the standard Snakemake environment variables. Cluster-specific variables are passed to jobs automatically.

## Log Files

By default, job logs are written to `.snakemake/sge_logs/` in your working directory:

- **Single jobs**: `{JOBID}.log` (stdout) and `{JOBID}.error` (stderr)
- **Array jobs**: `{JOBID}.{TASKID}.log` and `{JOBID}.{TASKID}.error`

Helper files (task manifests and scripts) are stored in `.snakemake/sge_logs/.meta/`.

You can customize the log directory:

```bash
snakemake --executor sge --sge-logdir custom_logs --jobs 100
```

Successful job logs are automatically deleted at workflow completion unless you set:

```bash
snakemake --executor sge --sge-keep-successful-logs --jobs 100
```

## Next Steps

See [further.md](further.md) for advanced topics including:
- Job array optimization and limits
- Cross-job dependency resolution
- Status polling and timeouts
- Troubleshooting and debugging
