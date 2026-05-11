# snakemake-executor-plugin-sge

A [Snakemake](https://snakemake.readthedocs.io) executor plugin for
**Sun Grid Engine (SGE)**, **Univa Grid Engine (UGE)**, and
**Open Grid Scheduler (OGS)** clusters.

## Features

- **Every job is an array job** – singletons become `-t 1-1`, batches become
  `-t 1-N`. One submission path, one tracking path, no special cases.
- **Group jobs as a single array job** – Snakemake group jobs (several rules
  bundled together) are submitted as one qsub call.
- **Rich qsub flag coverage** – queue, project, parallel environment, wall
  time, memory (per-slot and total), priority, requeue, reservation, notify,
  mail, hold_jid, task concurrency, env-var export, join logs, and arbitrary
  extra flags.
- **Per-rule resource overrides** – any global setting can be overridden via
  rule `resources:` (e.g. `sge_queue`, `sge_pe`, `sge_extra`, …).
- **Two-stage status polling** – `qstat` for in-flight jobs, `qacct` for
  completed jobs; configurable retries and optional qacct disabling.
- **Clean log management** – per-task stdout/stderr, optional auto-delete on
  success, age-based rotation.

## Installation

```bash
pip install snakemake-executor-plugin-sge
```

## Quick start

```bash
snakemake --executor sge --jobs 100
```

## Common options

| CLI flag | Default | Description |
|---|---|---|
| `--sge-queue` | — | Default queue (`-q`) |
| `--sge-pe` | — | Parallel environment for multi-thread jobs |
| `--sge-project` | — | Project (`-P`) |
| `--sge-export-env` | `True` | Export environment (`-V`) |
| `--sge-join-logs` | `False` | Merge stdout+stderr (`-j y`) |
| `--sge-requeue` | — | Requeue on failure (`-r y/n`) |
| `--sge-reservation` | — | Resource reservation (`-R y/n`) |
| `--sge-notify` | `False` | Send notify signal |
| `--sge-mail-on` | — | Mail events (`b`, `e`, `a`, `s`, `n`) |
| `--sge-mail-address` | — | Mail address (`-M`) |
| `--sge-hold-jid` | — | Hold until job(s) finish |
| `--sge-array-limit` | `75000` | Max tasks per `qsub -t` call |
| `--sge-task-concurrency` | — | Max concurrent array tasks (`-tc`) |
| `--sge-logdir` | `.snakemake/sge_logs` | Log directory |
| `--sge-keep-successful-logs` | `False` | Keep logs for successful jobs |
| `--sge-use-qacct` | `True` | Use `qacct` for finished-job detection |
| `--sge-extra` | — | Raw extra qsub flags (global) |
| `--sge-jobname-prefix` | — | Prefix for SGE job names |

## Per-rule resource overrides

```python
rule my_rule:
    resources:
        runtime        = 120,          # minutes → h_rt=02:00:00
        mem_mb         = 8192,         # total memory (divided by threads per slot)
        mem_mb_per_cpu = 2048,         # per-slot memory (takes precedence)
        threads        = 4,
        sge_queue      = "highmem.q",
        sge_pe         = "smp",
        sge_project    = "myproject",
        sge_extra      = "-l gpu=1",   # arbitrary extra qsub flags
        sge_resources  = {"h_cpu": "24:00:00", "arch": "lx-amd64"},
        sge_join_logs  = True,
        sge_requeue    = True,
        sge_priority   = -100,
        sge_notify     = True,
        sge_mail_on    = "e",
        sge_mail_address = "me@example.com",
        sge_hold_jid   = "12345",
        sge_task_concurrency = 8,
        sge_export_env = False,
    threads: 4
    shell: "..."
```

## Array-job design

Every qsub submission uses `qsub -t start-end`.  Execution commands are
stored in a JSON file on the shared filesystem before `qsub` is called.  The
submission script reads `$SGE_TASK_ID`, looks up the compressed command, and
evaluates it.  This avoids command-line length limits and all heredoc/quoting
problems.

## License

MIT
