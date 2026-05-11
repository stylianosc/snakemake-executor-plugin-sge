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


def _fmt_runtime(value) -> str:
    """Normalise a runtime value to 'HH:MM:SS' for SGE's h_rt.

    Accepts:
      - int / numeric: interpreted as minutes (Snakemake convention).
      - str 'HH:MM:SS' or 'HH:MM' or 'MM': kept as-is after parsing.
      - str of digits ('20'): treated as minutes.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        total_min = int(value)
        h, m = divmod(total_min, 60)
        return f"{h:02d}:{m:02d}:00"
    s = str(value).strip()
    if s.isdigit():
        total_min = int(s)
        h, m = divmod(total_min, 60)
        return f"{h:02d}:{m:02d}:00"
    parts = s.split(":")
    if len(parts) == 3:
        h, m, sec = parts
        return f"{int(h):02d}:{int(m):02d}:{int(sec):02d}"
    if len(parts) == 2:
        h, m = parts
        return f"{int(h):02d}:{int(m):02d}:00"
    # Last-ditch passthrough; let SGE complain if invalid.
    return s


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
    exec_cmd: Optional[str] = None,
    script_path: Optional[str] = None,
    is_array: bool = False,
    hold_jid_list: Optional[list] = None,
    hold_jid_ad_override: Optional[str] = None,
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
        array_range - SGE array range string (if is_array is True)
    settings:
        ExecutorSettings instance.
    exec_cmd:
        The execution command (if script_path is None).
    script_path:
        Path to the already-written bash submission script.
    is_array:
        Whether this is an array job.
    hold_jid_list:
        Auto-resolved whole-array upstream job IDs (passed via -hold_jid).
        Used when downstream tasks must wait for entire upstream arrays.
    hold_jid_ad_override:
        Single upstream array job ID passed via -hold_jid_ad. When set,
        task N of this array waits only on task N of the upstream array.
        Takes precedence over settings.hold_jid_ad.
    """
    log_dir   = Path(params.get("log_dir") or str(params.get("log_stdout", "")).rsplit("/", 1)[0])
    run_uuid  = params.get("run_uuid", "0000")
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
    # By default we do NOT pass -V: the submitted command already
    # includes a full snakemake invocation with all the environment it
    # needs, and inheriting the entire login shell environment can
    # mask issues or accidentally leak credentials onto compute nodes.
    # Opt in per-rule via resources.sge_export_env=True or globally via
    # --sge-export-env.
    export_env = (
        job.resources.get("sge_export_env")
        if job.resources.get("sge_export_env") is not None
        else getattr(settings, "export_env", False)
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
    # Accept either Snakemake's `runtime` (minutes) or a free-form
    # `time` resource that can be 'HH:MM:SS' / 'HH:MM' / minutes.
    runtime_raw = job.resources.get("runtime")
    if runtime_raw is None:
        runtime_raw = job.resources.get("time")
    runtime_fmt = _fmt_runtime(runtime_raw)
    if runtime_fmt is not None:
        call += f" -l h_rt={runtime_fmt}"

    # ── memory ────────────────────────────────────────────────────
    # SGE clusters typically gate on both h_vmem (virtual memory) AND
    # tmem (RAM, used by UCL's CS cluster).  Emit both from mem_mb so
    # users don't have to specify h_vmem / tmem separately.  Users can
    # still override either via sge_resources={'h_vmem': ..., 'tmem': ...}.
    mem_mb_per_cpu = job.resources.get("mem_mb_per_cpu")
    mem_mb         = job.resources.get("mem_mb")
    if mem_mb_per_cpu:
        mem_per_slot_str = _fmt_mem(mem_mb_per_cpu)
    elif mem_mb:
        per_slot = max(1, int(mem_mb) // max(1, threads))
        mem_per_slot_str = _fmt_mem(per_slot)
    else:
        mem_per_slot_str = None
    if mem_per_slot_str is not None:
        call += f" -l h_vmem={mem_per_slot_str}"
        call += f" -l tmem={mem_per_slot_str}"

    # ── scratch (temporary local disk) ────────────────────────────────
    # Snakemake convention: `disk_mb` / `scratch_size` in MB → tscratch.
    scratch_mb = (
        job.resources.get("tscratch")
        or job.resources.get("scratch_size")
        or job.resources.get("disk_mb")
    )
    if scratch_mb is not None:
        call += f" -l tscratch={_fmt_mem(scratch_mb)}"

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
    # Priority: explicit resource/setting > auto-resolved override > whole-array fallback.
    #
    # When hold_jid_ad_override is set, this submission is an array whose
    # per-task dependencies map 1:1 onto an upstream array.  In that case
    # -hold_jid_ad is sufficient by itself; emitting an additional
    # -hold_jid on the same upstream array would be redundant and could
    # cause the scheduler to wait for the entire upstream array even
    # when individual tasks finish early.
    hold_jid = (
        job.resources.get("sge_hold_jid")
        or getattr(settings, "hold_jid", None)
    )
    if hold_jid:
        call += f" -hold_jid {_safe(str(hold_jid))}"
    elif hold_jid_list and not hold_jid_ad_override:
        # Auto-resolved dependencies from Snakemake DAG (--immediate-submit)
        call += f" -hold_jid {','.join(hold_jid_list)}"

    hold_jid_ad = (
        job.resources.get("sge_hold_jid_ad")
        or hold_jid_ad_override
        or getattr(settings, "hold_jid_ad", None)
    )
    if hold_jid_ad:
        call += f" -hold_jid_ad {_safe(str(hold_jid_ad))}"

    # ── max concurrent array tasks ───────────────────────────────────
    task_concurrency = (
        job.resources.get("sge_task_concurrency")
        or getattr(settings, "task_concurrency", None)
    )
    if task_concurrency is not None:
        call += f" -tc {int(task_concurrency)}"

    # ── array range ───────────────────────────────────────────────────
    if is_array:
        array_range = params.get("array_range", "1-1")
        call += f" -t {array_range}"

    # ── extra qsub flags (per-rule or global) ──────────────────────────
    sge_extra = job.resources.get("sge_extra") or getattr(settings, "extra", None)
    if sge_extra:
        call += f" {sge_extra}"

    # ── submission script or command ────────────────────────────────────
    if script_path:
        call += f" {_safe(script_path)}"
    elif exec_cmd:
        # Pass the command via stdin if there is no script
        call = f"echo {_safe(exec_cmd)} | {call}"

    return call
