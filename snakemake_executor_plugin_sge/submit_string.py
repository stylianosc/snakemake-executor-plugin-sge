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


def _fmt_runtime(value) -> Optional[str]:
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


_OPTION_ALIASES = {
    "binding": "binding",
    "cwd": "cwd",
    "error": "e",
    "e": "e",
    "hard": "hard",
    "join": "j",
    "j": "j",
    "mail_options": "m",
    "m": "m",
    "email": "M",
    "M": "M",
    "notify": "notify",
    "now": "now",
    "name": "N",
    "N": "N",
    "output": "o",
    "o": "o",
    "project": "P",
    "P": "P",
    "priority": "p",
    "p": "p",
    "parallel_environment": "pe",
    "pe": "pe",
    "pty": "pty",
    "queue": "q",
    "q": "q",
    "reservation": "R",
    "R": "R",
    "rerun": "r",
    "r": "r",
    "shell": "S",
    "S": "S",
    "soft": "soft",
    "variable": "v",
    "v": "v",
    "export_env": "V",
    "V": "V",
    "workdir": "wd",
    "wd": "wd",
}

_RESOURCE_ALIASES = {
    "runtime": "h_rt",
    "time": "h_rt",
    "walltime": "h_rt",
    "cpu": "h_cpu",
    "mem_mb": "h_vmem",
    "mem": "h_vmem",
    "memory": "h_vmem",
    "virtual_memory": "h_vmem",
    "scratch_size": "tscratch",
    "disk_mb": "h_fsize",
}


def _ensure_option_path(option: str, value: str) -> None:
    """Create directories for path-based qsub options where relevant."""
    if not value or "$" in value:
        return

    path = Path(value)
    if option == "wd":
        path.mkdir(parents=True, exist_ok=True)
    elif option in {"o", "e"}:
        # SGE accepts either file path or directory (often with trailing slash).
        if value.endswith("/"):
            path.mkdir(parents=True, exist_ok=True)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)


def _normalize_sge_extra(sge_extra: Optional[str]) -> Optional[str]:
    """Normalize key/value aliases in sge_extra to canonical qsub flags.

    Examples:
      - output=/path/log -> -o /path/log
      - workdir=/path/wd -> -wd /path/wd
      - runtime=20:00:00 -> -l h_rt=20:00:00
    """
    if not sge_extra:
        return sge_extra

    try:
        tokens = shlex.split(str(sge_extra))
    except ValueError:
        return sge_extra

    normalized: list[str] = []
    idx = 0
    while idx < len(tokens):
        token = tokens[idx]

        # Preserve canonical qsub flags as-is.
        if token.startswith("-"):
            normalized.append(token)
            # Create dirs for explicit path flags where value is next token.
            if token in {"-wd", "-o", "-e"} and idx + 1 < len(tokens):
                _ensure_option_path(token[1:], tokens[idx + 1])
            elif token.startswith("-wd") and token != "-wd":
                _ensure_option_path("wd", token[3:])
            elif token.startswith("-o") and token != "-o":
                _ensure_option_path("o", token[2:])
            elif token.startswith("-e") and token != "-e":
                _ensure_option_path("e", token[2:])
            idx += 1
            continue

        # Convert alias=value syntax.
        if "=" in token:
            key, value = token.split("=", 1)
            canon_opt = _OPTION_ALIASES.get(key)
            if canon_opt:
                normalized.extend([f"-{canon_opt}", value])
                _ensure_option_path(canon_opt, value)
                idx += 1
                continue

            canon_res = _RESOURCE_ALIASES.get(key)
            if canon_res:
                normalized.extend(["-l", f"{canon_res}={value}"])
                idx += 1
                continue

        # Convert bare alias options (e.g. "cwd").
        canon_opt = _OPTION_ALIASES.get(token)
        if canon_opt and canon_opt in {"cwd", "hard", "notify", "now", "soft", "V"}:
            normalized.append(f"-{canon_opt}")
            idx += 1
            continue

        normalized.append(token)
        idx += 1

    return " ".join(_safe(t) if " " in t else t for t in normalized)


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
    workdir   = job.resources.get("workdir") or params.get("workdir", "")
    sge_extra_raw = job.resources.get("sge_extra") or getattr(settings, "extra", None)
    sge_extra = _normalize_sge_extra(sge_extra_raw)

    # SGE expects the stdout/stderr log directory to exist already.
    log_dir.mkdir(parents=True, exist_ok=True)
    if workdir:
        Path(workdir).mkdir(parents=True, exist_ok=True)

    # ── job name (max 64 chars, must start with letter) ────────────────────
    # Use rule name from resources (or job.name). If generic or missing, add a clear prefix with run UUID.
    job_rule = job.resources.get("name") if hasattr(job, "resources") and job.resources else None
    if not job_rule:
        job_rule = getattr(job, "name", "job")
        
    import re
    # SGE job names cannot contain slashes or start with numbers.
    # Replace anything not alphanumeric, hyphen, or underscore with underscore.
    safe_rule = re.sub(r"[^\w-]", "_", job_rule)
    if getattr(job, "is_group", lambda: False)():
        job_name = f"sm_grp_{safe_rule}_{run_uuid[:8]}"[:64]
    elif not job_rule or job_rule == "job":
        job_name = f"sm_job_{run_uuid[:8]}"[:64]
    else:
        job_name = f"{safe_rule}_{run_uuid[:8]}"[:64]

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
        f" -l stack=unlimited"
        f" -N {_safe(job_name)}"
    )

    # Stdout/stderr: specify exact log file paths using SGE variable expansion
    # $JOB_ID expands to the job ID, $TASK_ID expands to the array task index
    # This produces logs named like "1234567.1.log" (jobid.taskid.log)
    log_stdout_path = log_dir / "$JOB_ID.$TASK_ID.log"
    call += f" -o {_safe(str(log_stdout_path))}"
    if join_logs:
        call += " -j y"
    else:
        log_stderr_path = log_dir / "$JOB_ID.$TASK_ID.error"
        call += f" -e {_safe(str(log_stderr_path))}"

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
    if sge_extra:
        call += f" {sge_extra}"

    # ── submission script or command ────────────────────────────────────
    if script_path:
        call += f" {_safe(script_path)}"
    elif exec_cmd:
        # Pass the command via stdin if there is no script
        call = f"echo {_safe(exec_cmd)} | {call}"

    return call
