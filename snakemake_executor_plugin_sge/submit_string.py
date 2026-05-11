"""Build the qsub command string for SGE/UGE/OGS job submission.

SGE command-line reference (key flags used here)
-------------------------------------------------
  -N <name>          Job name
  -o <path>          Standard output file
  -e <path>          Standard error file
  -q <queue>         Queue / queue list
  -pe <pe> <slots>   Parallel environment (multi-threaded jobs)
  -P <project>       Project
  -l h_rt=HH:MM:SS   Hard wall-clock time limit
  -l h_vmem=<size>   Hard virtual memory limit per slot
  -V                 Export all environment variables
  -wd <dir>          Working directory
  -t <start>-<end>   Array-job task range
  -tc <concurrency>  Max concurrently running array tasks (optional)
"""

import shlex
from pathlib import Path
from typing import Optional


def _fmt_runtime(minutes: int) -> str:
    """Convert integer minutes to SGE h_rt format HH:MM:SS."""
    h = minutes // 60
    m = minutes % 60
    return f"{h:02d}:{m:02d}:00"


def _fmt_mem(mem_mb: int) -> str:
    """Convert integer MB to a qsub-friendly string (e.g. '4096M')."""
    if mem_mb >= 1024 and mem_mb % 1024 == 0:
        return f"{mem_mb // 1024}G"
    return f"{mem_mb}M"


def get_submit_command(
    job,
    params: dict,
    settings,
    exec_cmd: Optional[str],
    script_path: Optional[str] = None,
    is_array: bool = False,
) -> str:
    """Return the full qsub command string for *job*.

    Parameters
    ----------
    job:
        Snakemake job object (provides ``.resources``, ``.name``, etc.)
    params:
        Dictionary of submission parameters::

            run_uuid      – unique ID for the whole Snakemake run
            log_stdout    – stdout log path (may contain $JOB_ID placeholders)
            log_stderr    – stderr log path
            workdir       – workflow working directory
            array_range   – (array only) "start-end" string, e.g. "1-50"
            task_map_b64  – (array only) base64-encoded task map

    settings:
        ``ExecutorSettings`` instance from the plugin.
    exec_cmd:
        The shell command to execute (for single jobs). ``None`` for array
        jobs where the command is in *script_path*.
    script_path:
        Path to the submission script (array jobs).
    is_array:
        Whether this is an array job submission.
    """
    # ----- Job name -------------------------------------------------------
    # SGE job names must start with a letter and contain no spaces.
    # We use the run UUID and truncate to 64 characters (SGE default limit).
    job_name = f"sm_{params['run_uuid']}"[:64]

    call = (
        f"qsub"
        f" -V"
        f" -N {shlex.quote(job_name)}"
        f" -o {shlex.quote(str(params['log_stdout']))}"
        f" -e {shlex.quote(str(params['log_stderr']))}"
    )

    # ----- Working directory ----------------------------------------------
    if params.get("workdir"):
        call += f" -wd {shlex.quote(str(params['workdir']))}"

    # ----- Queue ----------------------------------------------------------
    # Per-rule resource takes precedence over the global setting
    queue = job.resources.get("sge_queue") or settings.queue
    if queue:
        call += f" -q {shlex.quote(str(queue))}"

    # ----- Project --------------------------------------------------------
    project = job.resources.get("sge_project") or settings.project
    if project:
        call += f" -P {shlex.quote(str(project))}"

    # ----- Parallel environment (threads) ---------------------------------
    threads = job.resources.get("threads", 1)
    pe = job.resources.get("sge_pe") or settings.pe
    if threads > 1:
        if pe:
            call += f" -pe {shlex.quote(str(pe))} {int(threads)}"
        else:
            # Still set the slots via -l to give the scheduler a hint
            call += f" -l slots={int(threads)}"
    else:
        # Single-threaded: no PE needed, but honour explicit -pe if given
        if pe:
            call += f" -pe {shlex.quote(str(pe))} 1"

    # ----- Wall-clock time ------------------------------------------------
    runtime_minutes = job.resources.get("runtime")
    if runtime_minutes is not None:
        call += f" -l h_rt={_fmt_runtime(int(runtime_minutes))}"

    # ----- Memory ---------------------------------------------------------
    mem_mb = job.resources.get("mem_mb")
    mem_mb_per_cpu = job.resources.get("mem_mb_per_cpu")
    if mem_mb_per_cpu:
        # SGE h_vmem is per-slot, so mem_mb_per_cpu maps directly
        call += f" -l h_vmem={_fmt_mem(int(mem_mb_per_cpu))}"
    elif mem_mb:
        # Convert total memory to per-slot
        per_slot_mb = max(1, int(mem_mb) // max(1, threads))
        call += f" -l h_vmem={_fmt_mem(per_slot_mb)}"

    # ----- Array job range ------------------------------------------------
    if is_array and params.get("array_range"):
        call += f" -t {params['array_range']}"

    # ----- Extra qsub flags (per-rule passthrough) -----------------------
    sge_extra = job.resources.get("sge_extra")
    if sge_extra:
        call += f" {sge_extra}"

    # ----- Command / script -----------------------------------------------
    if script_path:
        # Array job: pass the pre-written script
        call += f" {shlex.quote(script_path)}"
    elif exec_cmd:
        # Single job: use -b y to pass a command string directly,
        # or wrap in a heredoc via /dev/stdin
        escaped = exec_cmd.replace('"', '\\"')
        call += f' -b y {shlex.quote(exec_cmd)}'

    return call
