"""SGE job status querying via qstat and qacct.

SGE does not have a single command that reliably returns the status of both
running *and* recently completed jobs.  We therefore use a two-stage approach:

1. ``qstat -j <id>`` (or ``qstat -u '*'``)  → jobs that are *queued* or
   *running* show up here.
2. ``qacct -j <id>``                         → jobs that have *finished*
   (successfully or with an error) are visible here after they leave the
   scheduler.  qacct is optional: it may not be available on all clusters.

Status mapping
--------------
The function returns a dict mapping external_jobid → status string where
status is one of:

  ``"running"``   – job is queued or running
  ``"finished"``  – job completed with exit code 0
  ``"failed"``    – job completed with non-zero exit code or with an error
  ``None``        – job not found in either qstat or qacct output (treat as
                    still queued, i.e. not yet visible)
"""

import asyncio
import re
import shutil
import subprocess
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Availability checks
# ---------------------------------------------------------------------------

def is_qstat_available() -> bool:
    """Return True if ``qstat`` is in PATH."""
    return shutil.which("qstat") is not None


def is_qacct_available() -> bool:
    """Return True if ``qacct`` is in PATH."""
    return shutil.which("qacct") is not None


# ---------------------------------------------------------------------------
# qstat parsing
# ---------------------------------------------------------------------------

# SGE qstat -xml state codes
# r  – running
# qw – queued waiting
# t  – transferring (about to run)
# Rr – re-running (after node failure)
# s  – suspended
# S  – suspended by load threshold
# T  – threshold suspended
# h  – on hold
# Eqw– error in queued waiting
# Ec – error
_RUNNING_STATES = {"r", "t", "Rr", "s", "S", "T", "qw", "h", "hqw", "hRwq"}
_ERROR_STATES   = {"Eqw", "Ec", "E", "d", "dr", "dt", "dRr", "dT"}


def _poll_qstat(job_ids: List[str], logger) -> Dict[str, str]:
    """Return a {job_id: status} dict from qstat output.

    ``status`` is ``'running'`` for jobs in the queue (any state) or
    ``'failed'`` for jobs in an error state.  Jobs not returned by qstat
    are absent from the dict (they have finished or never existed).
    """
    result: Dict[str, str] = {}
    if not job_ids:
        return result

    # Use XML output for robust parsing
    try:
        raw = subprocess.check_output(
            "qstat -xml -u '*'",
            shell=True,
            text=True,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as exc:
        logger.warning(f"qstat failed: {exc.stderr.strip()}")
        return result

    # Parse each <job_list> element
    # We look for job number and state; we don't need full XML parsing.
    for block in re.finditer(
        r"<job_list[^>]*>(.*?)</job_list>", raw, re.DOTALL
    ):
        content = block.group(1)
        jn_m = re.search(r"<JB_job_number>(\d+)</JB_job_number>", content)
        st_m = re.search(r"<state>(\S+)</state>", content)
        if not jn_m:
            continue
        base_id = jn_m.group(1)
        state = st_m.group(1) if st_m else "unknown"

        # Match against full IDs (may include task suffix like 12345.3)
        matched_ids = [
            jid for jid in job_ids
            if jid == base_id or jid.startswith(f"{base_id}.")
        ]
        for jid in matched_ids:
            if any(s in state for s in _ERROR_STATES):
                result[jid] = "failed"
            else:
                result[jid] = "running"

    return result


# ---------------------------------------------------------------------------
# qacct parsing
# ---------------------------------------------------------------------------

def _poll_qacct(job_id: str, logger) -> Optional[str]:
    """Return ``'finished'`` or ``'failed'`` for *job_id* via qacct.

    Returns ``None`` if the job is not in the accounting database yet.

    For array jobs (``12345.3``) we query the parent ID and task.
    """
    base_id = job_id.split(".")[0]
    try:
        raw = subprocess.check_output(
            f"qacct -j {base_id}",
            shell=True,
            text=True,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError:
        # Job not in accounting yet
        return None

    # If this is a task ID, filter to that task only
    task_id = job_id.split(".")[1] if "." in job_id else None

    # Split into per-task blocks (separated by dashes)
    blocks = re.split(r"={10,}", raw)
    for block in blocks:
        if not block.strip():
            continue
        # If task_id specified, only look at matching task block
        if task_id:
            m = re.search(r"taskid\s+(\d+)", block)
            if not m or m.group(1) != task_id:
                continue
        exit_m = re.search(r"exit_status\s+(\d+)", block)
        failed_m = re.search(r"failed\s+(\d+)", block)
        if exit_m:
            exit_code = int(exit_m.group(1))
            failed_flag = int(failed_m.group(1)) if failed_m else 0
            if exit_code == 0 and failed_flag == 0:
                return "finished"
            else:
                return "failed"

    return None


# ---------------------------------------------------------------------------
# Main status query coroutine
# ---------------------------------------------------------------------------

async def query_job_status(
    active_jobs,
    use_qacct: bool,
    logger,
    submit_times: Optional[Dict[str, float]] = None,
) -> Optional[Dict[str, str]]:
    """Asynchronously query qstat (and optionally qacct) for all active jobs."""
    import time
    job_ids = [j.external_jobid for j in active_jobs]
    if not job_ids:
        return {}

    loop = asyncio.get_event_loop()

    try:
        qstat_status = await loop.run_in_executor(
            None, _poll_qstat, job_ids, logger
        )
    except Exception as exc:
        logger.warning(f"qstat query raised an exception: {exc}")
        return None

    status_map: Dict[str, str] = {}

    for jid in job_ids:
        # qstat is the ground truth for currently running jobs
        if jid in qstat_status:
            status_map[jid] = qstat_status[jid]
            continue

        # If it's not in qstat, it might have finished or hasn't appeared yet.
        # Check grace period first!
        submit_time = submit_times.get(jid, 0) if submit_times else 0
        if time.time() - submit_time < 20:
            # Job is too young to be considered finished, even if it's not in qstat
            # or if qacct returns an old reused job ID.
            continue  # leave it absent from status_map -> treats as queued/running

        # Job is old enough. We can now trust qacct or assume it's finished.
        if use_qacct and is_qacct_available():
            try:
                acct_status = await loop.run_in_executor(
                    None, _poll_qacct, jid, logger
                )
            except Exception as exc:
                logger.warning(f"qacct query for {jid} raised: {exc}")
                acct_status = None

            if acct_status is not None:
                status_map[jid] = acct_status
            else:
                pass
        else:
            # qacct disabled or unavailable: assume finished since it's old enough
            # and no longer in qstat.
            status_map[jid] = "finished"

    return status_map
