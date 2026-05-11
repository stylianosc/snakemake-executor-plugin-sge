"""
Build the qsub command string for SGE/UGE/OGS.

Every job – whether a single rule invocation, a rule batch, or a Snakemake
group job – is always submitted as an array job (qsub -t start-end).  For a
single task this becomes -t 1-1.  The execution commands for each task are
stored in a JSON file on the shared filesystem, written at submission time.
The submission script reads that file keyed on $SGE_TASK_ID, decompresses
the command, and evaluates it.  This avoids command-line length limits and
heredoc/quoting issues.

SGE flags covered
─────────────────
  -N  <name>               Job name
  -o  <path>               Stdout path  (directory – SGE appends job/task id)
  -e  <path>               Stderr path
  -j  y|n                  Merge stdout+stderr
  -cwd / -wd <dir>         Working directory
  -V  / -v VAR=val         Export environment
  -S  /bin/bash            Shell
  -q  <queue>              Queue / queue-list
  -P  <project>            Project
  -pe <pe> <slots>         Parallel environment (multi-thread)
  -l  h_rt=HH:MM:SS        Hard wall-clock limit
  -l  h_vmem=<N>M|G        Hard virtual-memory per slot
  -l  <key>=<val>          Any extra -l resource (sge_resources dict)
  -p  <priority>           Job priority  (-1023 … 1024)
  -r  y|n                  Requeue after failure
  -R  y|n                  Resource reservation
  -notify                  Send USR1/USR2 before KILL/STOP
  -m  <flags>              Mail on: b, e, a, s, n
  -M  <address>            Mail address
  -hold_jid <id>,...       Hold until these jobs finish
  -tc <N>                  Max concurrently running array tasks
  -t  <start>-<end>        Array task range
  <extra>                  sge_extra / --sge-extra pass-through
"""

import shlex
from pathlib import Path
from typing import Optional


def _fmt_runtime(minutes: int) -> str:
    """Convert integer minutes → 'HH:MM:SS' for h_rt."""
    h, m = divmod(int(minutes), 60)
    return f"{h:02d}:{m:02d}:00"


def _fmt_mem(mem_mb: int) -> str:
    """Convert MB → 'NG' when evenly divisible, else 'NM'."""
    mem_mb = int(mem_mb)
    if mem_mb >= 1024 and mem_mb % 1024 == 0:
        return f"{mem_mb // 1024}G"
    return f"{mem_mb}M"


def _safe(value) -> str:
    """shlex-quote a value, converting to str first."""
    return shlex.quote(str(value))


def get_submit_command(
    job,
    params: dict,
    settings,
    script_path: str,
    array_range: str,
) -> str:
    """Return the complete qsub command string.

    Parameters
    ----------
    job:
        Snakemake JobExecutorInterface (provides .resources, .name, etc.)
    params:
        run_uuid    – unique ID for the whole Snakemake run
        log_dir     – directory for stdout/stderr logs
        workdir     – workflow working directory
    settings:
        ExecutorSettings instance.
    script_path:
        Path to the already-written bash submission script.
    array_range:
        SGE array range string, e.g. '1-50' or '1-1'.
    """
    log_dir   = Path(params["log_dir"])
    run_uuid  = params["run_uuid"]
    workdir   = params.get("workdir", "")

    # ── job name (max 64 chars, must start with letter) ────────────────────
    # 'sm_' prefix ensures we never start with a digit.
    job_name = f"sm_{run_uuid}"[:64]

    # ── merge logs? ─────────────────────────────────────────────────
    join_logs = bool(
        job.resources.get("sge_join_logs")
        if job.resources.get("sge_join_logs") is not None
        else getattr(settings, "join_logs", False)
    )

    # ── assemble call ─────────────────────────────────────────────────
    call = (
        f"qsub"
        f" -S /bin/bash"
        f" -N {_safe(job_name)}"
    )

    # Stdout/stderr: pass the log *directory*; SGE appends .oJOBID.TASKID
    # Use -o dir/ -e dir/ (trailing slash = treat as directory on most SGE)
    call += f" -o {_safe(str(log_dir) + '/')}"
    if join_logs:
        call += " -j y"
    else:
        call += f" -e {_safe(str(log_dir) + '/')}"

    # ── working directory ──────────────────────────────────────────────
    if workdir:
        call += f" -wd {_safe(workdir)}"
    else:
        call += " -cwd"

    # ── environment export ───────────────────────────────────────────
    export_env = (
        job.resources.get("sge_export_env")
        if job.resources.get("sge_export_env") is not None
        else getattr(settings, "export_env", True)
    )
    if export_env:
        call += " -V"
    extra_envvars = getattr(settings, "extra_envvars", None)
    if extra_envvars:
        for kv in extra_envvars.split(","):
            kv = kv.strip()
            if kv:
                call += f" -v {_safe(kv)}"

    # ── queue ───────────────────────────────────────────────────────
    queue = job.resources.get("sge_queue") or getattr(settings, "queue", None)
    if queue:
        call += f" -q {_safe(queue)}"

    # ── project ───────────────────────────────────────────────────
    project = job.resources.get("sge_project") or getattr(settings, "project", None)
    if project:
        call += f" -P {_safe(project)}"

    # ── parallel environment / threads ────────────────────────────────
    threads = int(job.resources.get("threads", 1))
    pe = job.resources.get("sge_pe") or getattr(settings, "pe", None)
    if threads > 1:
        if pe:
            call += f" -pe {_safe(pe)} {threads}"
        else:
            call += f" -l slots={threads}"
    elif pe:
        call += f" -pe {_safe(pe)} 1"

    # ── wall-clock time ─────────────────────────────────────────────
    runtime_minutes = job.resources.get("runtime")
    if runtime_minutes is not None:
        call += f" -l h_rt={_fmt_runtime(runtime_minutes)}"

    # ── memory ────────────────────────────────────────────────────
    mem_mb_per_cpu = job.resources.get("mem_mb_per_cpu")
    mem_mb         = job.resources.get("mem_mb")
    if mem_mb_per_cpu:
        call += f" -l h_vmem={_fmt_mem(mem_mb_per_cpu)}"
    elif mem_mb:
        per_slot = max(1, int(mem_mb) // max(1, threads))
        call += f" -l h_vmem={_fmt_mem(per_slot)}"

    # ── extra -l resources (dict of key→value) ────────────────────────
    sge_resources = job.resources.get("sge_resources") or {}
    if isinstance(sge_resources, str):
        sge_resources = dict(
            kv.split("=", 1)
            for kv in sge_resources.split(",")
            if "=" in kv
        )
    for k, v in sge_resources.items():
        call += f" -l {_safe(k)}={_safe(v)}"

    # ── priority ───────────────────────────────────────────────────
    priority = job.resources.get("sge_priority") or getattr(settings, "priority", None)
    if priority is not None:
        call += f" -p {int(priority)}"

    # ── requeue ──────────────────────────────────────────────────
    requeue = (
        job.resources.get("sge_requeue")
        if job.resources.get("sge_requeue") is not None
        else getattr(settings, "requeue", None)
    )
    if requeue is not None:
        call += f" -r {'y' if requeue else 'n'}"

    # ── reservation ───────────────────────────────────────────────
    reservation = (
        job.resources.get("sge_reservation")
        if job.resources.get("sge_reservation") is not None
        else getattr(settings, "reservation", None)
    )
    if reservation is not None:
        call += f" -R {'y' if reservation else 'n'}"

    # ── notify ────────────────────────────────────────────────────
    notify = (
        job.resources.get("sge_notify")
        if job.resources.get("sge_notify") is not None
        else getattr(settings, "notify", False)
    )
    if notify:
        call += " -notify"

    # ── mail ──────────────────────────────────────────────────────
    mail_on = (
        job.resources.get("sge_mail_on")
        or getattr(settings, "mail_on", None)
    )
    if mail_on:
        call += f" -m {_safe(mail_on)}"
    mail_addr = (
        job.resources.get("sge_mail_address")
        or getattr(settings, "mail_address", None)
    )
    if mail_addr:
        call += f" -M {_safe(mail_addr)}"

    # ── job hold ──────────────────────────────────────────────────
    hold_jid = (
        job.resources.get("sge_hold_jid")
        or getattr(settings, "hold_jid", None)
    )
    if hold_jid:
        call += f" -hold_jid {_safe(str(hold_jid))}"

    # ── max concurrent array tasks ───────────────────────────────────
    task_concurrency = (
        job.resources.get("sge_task_concurrency")
        or getattr(settings, "task_concurrency", None)
    )
    if task_concurrency is not None:
        call += f" -tc {int(task_concurrency)}"

    # ── array range (always present, even for singletons: -t 1-1) ────────
    call += f" -t {array_range}"

    # ── extra qsub flags (per-rule or global) ──────────────────────────
    sge_extra = job.resources.get("sge_extra") or getattr(settings, "extra", None)
    if sge_extra:
        call += f" {sge_extra}"

    # ── submission script ───────────────────────────────────────────────
    call += f" {_safe(script_path)}"

    return call
