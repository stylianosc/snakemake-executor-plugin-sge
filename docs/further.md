# Advanced Topics

## Job Arrays

### How They Work

Snakemake group jobs (created with `group:` directive) are automatically submitted as SGE array jobs when using this executor. Array jobs significantly reduce scheduler overhead compared to submitting individual `qsub` commands.

For example, if your workflow has a group with 100 tasks:
- **Without array jobs**: 100 individual `qsub` calls
- **With array jobs**: 1 `qsub -t 1-100` call

### Array Limits

The executor respects SGE's maximum array size through the `array_limit` setting:

```bash
snakemake --executor sge --sge-array-limit 75000 --jobs 100
```

Default: 75,000 tasks per array. If a group exceeds this limit, multiple array submissions are automatically performed.

### Task Encoding

Array tasks are encoded as zlib-compressed, base64-encoded commands in a shared task map file. This approach:
- Avoids ARG_MAX shell argument limits
- Allows any task size (large commands are handled gracefully)
- Stores the map in `.sge_logs/.meta/{group|rule}/task_map.b64`
- Includes a human-readable manifest in `.sge_logs/.meta/{group|rule}/task_manifest.json`

## Cross-Job Dependencies

### Dependency Resolution

The executor automatically translates Snakemake's DAG dependencies to SGE dependencies:

- **Within a rule**: If all tasks have matching upstreams in a single array job, the executor uses `qsub -hold_jid_ad` for per-task 1:1 dependencies
- **Multiple rules**: Falls back to `qsub -hold_jid` to wait for entire upstream job(s)
- **Immediate submit**: The executor maintains an in-memory mapping of Snakemake jobs to SGE job IDs for `--immediate-submit` mode

### Manual Dependencies

You can also manually hold jobs on upstream SGE job IDs:

```bash
snakemake --executor sge --sge-hold-jid 12345 --jobs 100
```

This is useful when coordinating with external SGE jobs.

## Status Polling

### Query Strategy

The executor polls job status using a combination of `qstat` and `qacct`:

- **qstat**: Fast, reports running and queued jobs
- **qacct**: Slower, reports completed and failed jobs
- **Combined**: The executor queries both to accurately track job states

Initial delay before first poll:

```bash
snakemake --executor sge --sge-init-seconds-before-status-checks 20 --jobs 100
```

Default: 20 seconds (SGE schedulers are typically fast; adjust if needed).

### Disabling qacct

If your cluster has a slow or unavailable `qacct`:

```bash
snakemake --executor sge --sge-disable-qacct --jobs 100
```

Note: Without `qacct`, the executor may not detect completed jobs as quickly.

### Retry Logic

Status check attempts before reporting a job as stuck:

```bash
snakemake --executor sge --sge-status-attempts 5 --jobs 100
```

Default: 5 attempts. Increase if your cluster has temporary qstat/qacct outages.

## Log File Management

### Directory Structure

All logs are stored in `.sge_logs/` by default:

```
.sge_logs/
├── 12345.log               # Single job stdout
├── 12345.error             # Single job stderr
├── 12346.1.log             # Array job task 1 stdout
├── 12346.1.error           # Array job task 1 stderr
├── 12346.2.log             # Array job task 2 stdout
├── 12346.2.error           # Array job task 2 stderr
└── .meta/
    ├── rule_align/
    │   ├── task_map.b64    # Encoded task commands
    │   └── task_manifest.json
    └── group_process/
        ├── task_map.b64
        └── task_manifest.json
```

### Log Cleanup

Logs for successful jobs are automatically deleted at workflow completion. To keep them:

```bash
snakemake --executor sge --sge-keep-successful-logs --jobs 100
```

### Automatic Cleanup of Old Logs

Old logs are cleaned up automatically based on:

```bash
snakemake --executor sge --sge-delete-logfiles-older-than 10 --jobs 100
```

Default: 10 days. Set to 0 or negative to disable.

## Queue and Project Assignment

### Static Configuration

Specify default queue and project:

```bash
snakemake --executor sge --sge-queue high.q --sge-project myproject --jobs 100
```

### Per-Rule Override

Override in individual rules:

```python
rule expensive:
    input: "data.txt"
    output: "result.txt"
    resources:
        sge_queue="high.q",
        sge_project="urgent",
    shell:
        "expensive_computation.sh {input} {output}"
```

### Parallel Environments

For multi-threaded jobs, specify a parallel environment:

```bash
snakemake --executor sge --sge-pe "smp 4" --jobs 100
```

Or per-rule:

```python
rule parallel_task:
    threads: 8
    resources:
        sge_pe="smp",  # Will be paired with thread count automatically
    shell:
        "parallel_tool {threads} {input} {output}"
```

## Job Naming

Add a prefix to all SGE job names for easier tracking:

```bash
snakemake --executor sge --sge-jobname-prefix "analysis_" --jobs 100
```

This will submit jobs with names like `analysis_uuid_xxxx` instead of just `uuid_xxxx`.

## Troubleshooting

### Check Job Status

List all submitted jobs:

```bash
qstat
```

Check a specific job:

```bash
qstat -j 12345
```

View finished job accounting:

```bash
qacct -j 12345
```

### View Logs

Check stdout and stderr:

```bash
cat .sge_logs/12345.log
cat .sge_logs/12345.error
```

For array jobs:

```bash
cat .sge_logs/12345.1.log   # Task 1
cat .sge_logs/12345.2.error # Task 2 stderr
```

### Common Issues

**"qstat: command not found"**
- SGE client tools are not in your PATH
- Load the SGE environment module or add SGE binaries to PATH

**Jobs not starting**
- Check queue availability: `qconf -sql`
- Verify resource requests (memory, runtime) don't exceed limits
- Check project membership: `qconf -sprj` and `qconf -sprjl`

**Slow status polling**
- If `qacct` is very slow, disable it: `--sge-disable-qacct`
- Increase initial delay: `--sge-init-seconds-before-status-checks 30`

**Array job failures**
- Check the task manifest: `.sge_logs/.meta/rule_name/task_manifest.json`
- View array job script: `.sge_logs/.meta/rule_name/array_job_*.sh`
- Check individual task logs for error details

## Performance Tips

1. **Use array jobs**: Always prefer Snakemake group jobs for similar tasks
2. **Batch submissions**: Use `--jobs` to control submission rate (default is unlimited)
3. **Tune status polling**: Adjust `init_seconds_before_status_checks` based on your cluster's speed
4. **Monitor logs**: Disable log cleanup initially to diagnose issues: `--sge-keep-successful-logs`
5. **Resource requests**: Be realistic with memory and runtime to avoid queue delays

## See Also

- [Snakemake documentation](https://snakemake.readthedocs.io/)
- [SGE user guide](http://gridscheduler.sourceforge.net/htmlman/htmlman1/qsub.html)
- [Plugin repository](https://github.com/stylianosc/snakemake-executor-plugin-sge)
