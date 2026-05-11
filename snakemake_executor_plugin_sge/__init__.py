"""Snakemake executor plugin for Sun Grid Engine (SGE/UGE/OGS).

This module is the main entry point for the plugin.  It exposes:
  - ExecutorSettings  – all user-facing configuration options
  - common_settings   – static metadata consumed by the Snakemake framework
  - Executor          – the RemoteExecutor subclass that drives qsub/qstat/qdel

Design philosophy
-----------------
The implementation closely mirrors snakemake-executor-plugin-slurm so that
anyone already familiar with that plugin can read and extend this one.  SGE
differences (array-job syntax, status polling via qstat, resource flags) are
isolated in helper modules:

  submit_string.py       – builds the qsub command string
  job_status_query.py    – wraps qstat/qacct polling
  job_cancellation.py    – wraps qdel

Array jobs for group jobs
-------------------------
Group jobs (Snakemake jobs that bundle several rule invocations) are
submitted as a single SGE array job (``qsub -t 1-N``).  Each task unpacks
its own execution command from a zlib-compressed, base64-encoded map that
is baked into the submission script via an environment variable.  This
reduces scheduler overhead and mirrors the SLURM plugin behaviour.
"""

__author__ = "Stylianos Serghiou"
__copyright__ = "Copyright 2025, Stylianos Serghiou"
__license__ = "MIT"

import atexit
import asyncio
import base64
import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator, List, Optional
import re
import shlex
import subprocess
import time
import uuid
import zlib

from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.settings import (
    CommonSettings,
    ExecutorSettingsBase,
)
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface
from snakemake_interface_common.exceptions import WorkflowError

from .submit_string import get_submit_command
from .job_status_query import query_job_status, is_qstat_available, is_qacct_available
from .job_cancellation import cancel_sge_jobs


# ---------------------------------------------------------------------------
# ExecutorSettings
# ---------------------------------------------------------------------------

@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    """User-facing settings for the SGE executor plugin.

    All fields map to ``--sge-<field_name>`` CLI flags when consumed by
    Snakemake's plugin interface.
    """

    # ---- Queue / scheduling -----------------------------------------------

    queue: Optional[str] = field(
        default=None,
        metadata={
            "help": (
                "SGE queue to submit jobs to (-q flag). "
                "Can also be set per-rule via the 'sge_queue' resource."
            ),
            "env_var": False,
            "required": False,
        },
    )

    pe: Optional[str] = field(
        default=None,
        metadata={
            "help": (
                "SGE parallel environment name used for multi-threaded jobs "
                "(-pe <pe> <threads>).  Must match a PE defined on your cluster. "
                "If unset, multi-threaded jobs are submitted without a PE "
                "(may fail on strict clusters)."
            ),
            "env_var": False,
            "required": False,
        },
    )

    project: Optional[str] = field(
        default=None,
        metadata={
            "help": (
                "SGE project to charge jobs to (-P flag). "
                "Can also be set per-rule via the 'sge_project' resource."
            ),
            "env_var": False,
            "required": False,
        },
    )

    # ---- Array jobs -------------------------------------------------------

    disable_group_jobs_as_array: bool = field(
        default=False,
        metadata={
            "help": (
                "Disable submitting Snakemake group jobs as SGE array jobs. "
                "By default, group jobs are submitted as array jobs (qsub -t 1-N), "
                "which reduces scheduler overhead. Set this flag to fall back to "
                "individual qsub calls per task."
            ),
            "env_var": False,
            "required": False,
        },
    )

    @property
    def group_jobs_as_array(self) -> bool:
        return not self.disable_group_jobs_as_array

    array_limit: int = field(
        default=75000,
        metadata={
            "help": (
                "Maximum number of array tasks per qsub -t call. "
                "If a group exceeds this limit, multiple array submissions are "
                "performed.  The default (75 000) is a conservative value that "
                "fits within SGE's typical MaxArraySize.  Adjust to match your "
                "cluster's configured limit."
            ),
            "env_var": False,
            "required": False,
        },
    )

    # ---- Logging ----------------------------------------------------------

    logdir: Optional[Path] = field(
        default=None,
        metadata={
            "help": (
                "Directory for SGE log files.  Defaults to "
                "'.snakemake/sge_logs' relative to the working directory. "
                "Absolute paths are used as-is; relative paths are resolved "
                "against the workflow working directory."
            ),
            "env_var": False,
            "required": False,
        },
    )

    keep_successful_logs: bool = field(
        default=False,
        metadata={
            "help": (
                "By default, log files for successful jobs are deleted at the "
                "end of the workflow.  Set this flag to preserve them."
            ),
            "env_var": False,
            "required": False,
        },
    )

    delete_logfiles_older_than: int = field(
        default=10,
        metadata={
            "help": (
                "Delete SGE log files older than this many days (default: 10). "
                "Set to 0 or negative to disable automatic deletion."
            ),
            "env_var": False,
            "required": False,
        },
    )

    hold_jid: Optional[str] = field(
        default=None,
        metadata={
            "help": "Hold this job until the specified SGE job IDs have finished.",
            "env_var": False,
            "required": False,
        },
    )

    hold_jid_ad: Optional[str] = field(
        default=None,
        metadata={
            "help": "Hold this array job until the corresponding array tasks of the specified SGE job IDs have finished.",
            "env_var": False,
            "required": False,
        },
    )

    # ---- Status polling ---------------------------------------------------

    init_seconds_before_status_checks: int = field(
        default=20,
        metadata={
            "help": (
                "Seconds to wait after job submission before the first "
                "qstat/qacct status poll.  SGE schedulers are usually faster "
                "than SLURM so 20 s is a reasonable default."
            ),
            "env_var": False,
            "required": False,
        },
    )

    status_attempts: int = field(
        default=5,
        metadata={
            "help": (
                "Number of consecutive qstat/qacct query attempts before "
                "giving up on a status check cycle."
            ),
            "env_var": False,
            "required": False,
        },
    )

    disable_qacct: bool = field(
        default=False,
        metadata={
            "help": (
                "Disable using qacct (accounting) in addition to qstat to detect "
                "completed / failed jobs. Use this if qacct is not available "
                "or is very slow on your cluster."
            ),
            "env_var": False,
            "required": False,
        },
    )

    @property
    def use_qacct(self) -> bool:
        return not self.disable_qacct

    # ---- Misc -------------------------------------------------------------

    jobname_prefix: str = field(
        default="",
        metadata={
            "help": (
                "Optional prefix prepended to the SGE job name. "
                "Must contain only alphanumeric characters, underscores, or "
                "hyphens.  Maximum 30 characters."
            ),
            "env_var": False,
            "required": False,
        },
    )

    def __post_init__(self) -> None:
        if self.jobname_prefix and not re.match(
            r"^[A-Za-z0-9_-]{1,30}$", self.jobname_prefix
        ):
            raise WorkflowError(
                "sge jobname_prefix must contain only alphanumeric characters, "
                "underscores or hyphens and must not exceed 30 characters."
            )
        if self.array_limit < 1:
            raise WorkflowError("sge array_limit must be at least 1.")


# ---------------------------------------------------------------------------
# CommonSettings – static metadata consumed by the Snakemake framework
# ---------------------------------------------------------------------------

common_settings = CommonSettings(
    non_local_exec=True,
    implies_no_shared_fs=False,
    job_deploy_sources=False,
    pass_default_storage_provider_args=True,
    pass_default_resources_args=True,
    pass_envvar_declarations_to_cmd=False,
    auto_deploy_default_storage_provider=False,
    # Wait 30s before the first status poll so SGE has time to register
    # newly submitted jobs in qstat.  Without this the wait thread polls
    # immediately and sees an empty qstat, marking jobs as finished.
    init_seconds_before_status_checks=30,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_logdir(workflow) -> Path:
    """Return the resolved path to the SGE log directory."""
    logdir = workflow.executor_settings.logdir
    if logdir and str(logdir).startswith("/"):
        return Path(logdir)
    elif logdir:
        return Path(workflow.workdir_init) / logdir
    else:
        return (Path(workflow.workdir_init) / ".snakemake" / "sge_logs").resolve()


def _get_job_wildcards(job: JobExecutorInterface) -> str:
    """Return a filesystem-safe wildcard string for a job."""
    wc = getattr(job, "wildcards", None)
    if wc is None:
        return ""
    parts = []
    for k, v in sorted(wc.items()):
        safe_v = re.sub(r"[^\w.-]", "_", str(v))
        parts.append(f"{k}={safe_v}")
    return "__".join(parts)


def _wildcard_sort_key(job: JobExecutorInterface):
    """Return a stable sort key derived from a job's wildcards.

    Used to assign deterministic SGE array task indices: two rules that
    iterate the same wildcard space (e.g. {subject}) will produce the
    same ordering, which is a precondition for -hold_jid_ad.
    """
    wc = getattr(job, "wildcards", None)
    if not wc:
        # Fall back to jobid so order is at least deterministic per run
        return ((), getattr(job, "jobid", 0))
    return (tuple(sorted((k, str(v)) for k, v in wc.items())), 0)


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------

class Executor(RemoteExecutor):
    """Snakemake executor that submits jobs to an SGE/UGE/OGS cluster.

    The executor lifecycle mirrors snakemake-executor-plugin-slurm:

    1.  ``run_jobs``          – classify incoming jobs and dispatch to either
                                ``run_job`` (single qsub) or
                                ``run_array_job`` (qsub -t 1-N).
    2.  ``check_active_jobs`` – poll qstat / qacct and report success/failure.
    3.  ``cancel_jobs``       – qdel all still-running jobs on interrupt.
    """

    def __init__(self, workflow, logger):
        super().__init__(workflow, logger)

    def __post_init__(self, test_mode: bool = False) -> None:
        self.test_mode = test_mode
        self.run_uuid = str(uuid.uuid4())
        if self.workflow.executor_settings.jobname_prefix:
            self.run_uuid = "_".join(
                [self.workflow.executor_settings.jobname_prefix, self.run_uuid]
            )
        self.logger.info(f"SGE run ID: {self.run_uuid}")

        self.sge_logdir = _resolve_logdir(self.workflow)
        self.sge_logdir.mkdir(parents=True, exist_ok=True)

        self._job_submission_executor = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="sge_job_submit"
        )
        self._main_event_loop: Optional[asyncio.AbstractEventLoop] = None

        # Track submitted job IDs for cancellation
        self._submitted_job_ids: List[str] = []

        # Authoritative mapping from a Snakemake job to its SGE submission.
        # Each entry is (sge_jobid, task_idx) where task_idx is None for
        # non-array (single qsub) submissions or the 1-based array task
        # index for array submissions.  Used to resolve cross-job
        # dependencies under --immediate-submit without depending on
        # Snakemake's persistence layer (which doesn't always have the
        # external_jobid populated by the time downstream submissions
        # need it).
        self._job_to_sge: "dict[JobExecutorInterface, tuple]" = {}

        atexit.register(self.clean_old_logs)

        # Warn if neither qstat nor qacct is available
        if not is_qstat_available():
            raise WorkflowError(
                "'qstat' is not available on this system. "
                "Please ensure that SGE/UGE client tools are in PATH."
            )

    # ------------------------------------------------------------------
    # Thread-safe report helpers
    # ------------------------------------------------------------------

    def _report_submission_threadsafe(self, job_info: SubmittedJobInfo) -> None:
        if self._main_event_loop is not None:
            self._main_event_loop.call_soon_threadsafe(
                self.report_job_submission, job_info
            )
        else:
            self.report_job_submission(job_info)

    def _report_error_threadsafe(
        self, job_info: SubmittedJobInfo, msg: str
    ) -> None:
        if self._main_event_loop is not None:
            self._main_event_loop.call_soon_threadsafe(
                self.report_job_error, job_info, msg
            )
        else:
            self.report_job_error(job_info, msg=msg)

    # ------------------------------------------------------------------
    # Job dispatch
    # ------------------------------------------------------------------

    def run_jobs(self, jobs: List[JobExecutorInterface]) -> None:
        """Classify and dispatch incoming jobs.

        Strategy
        --------
        Regular (non-group) jobs from the same rule arriving in one batch are
        bucketed and submitted as a single SGE array job (qsub -t).  This
        dramatically reduces scheduler overhead when many instances of the
        same rule are submitted at once (e.g. one task per subject under
        --immediate-submit).  Single-instance rules become a 1-task array.

        Group jobs follow the existing logic: bundled into a single array
        when ``group_jobs_as_array`` is enabled, individually otherwise.
        """
        if self._main_event_loop is None:
            try:
                self._main_event_loop = asyncio.get_running_loop()
            except RuntimeError:
                self._main_event_loop = None

        # Separate group jobs from regular jobs.  Regular jobs are bucketed
        # by rule name and sorted by wildcards within each bucket so that
        # task indices are deterministic AND consistent across rules that
        # share the same wildcards (e.g. {subject}).  This is what enables
        # -hold_jid_ad: when downstream task N is held on upstream task N,
        # the two tasks must refer to the same logical unit.
        group_jobs: List[JobExecutorInterface] = []
        # Use a dict to bucket regular jobs by rule name. Python preserves
        # insertion order, so the first-seen rule is submitted first.
        regular_buckets: "dict[str, List[JobExecutorInterface]]" = {}
        for job in jobs:
            if job.is_group():
                group_jobs.append(job)
            else:
                regular_buckets.setdefault(job.name, []).append(job)
        for bucket in regular_buckets.values():
            bucket.sort(key=_wildcard_sort_key)

        # With --immediate-submit, each report_job_submission call releases
        # the scheduler semaphore.  If we dispatch to threads, the scheduler
        # can race ahead before all jobs in this batch are registered,
        # concluding the workflow is stuck.  Submit synchronously instead.
        immediate = self.workflow.remote_execution_settings.immediate_submit

        # Submit each per-rule bucket as one array job.
        for rule_name, bucket in regular_buckets.items():
            if immediate:
                self.run_array_job(bucket)
            else:
                self._job_submission_executor.submit(
                    self.run_array_job, bucket
                )

        # Submit group jobs
        if group_jobs:
            settings = self.workflow.executor_settings
            if settings.group_jobs_as_array and len(group_jobs) > 1:
                # Package all group tasks as a single array job
                if immediate:
                    self.run_array_job(group_jobs)
                else:
                    self._job_submission_executor.submit(
                        self.run_array_job, group_jobs
                    )
            else:
                # Fallback: submit each group job task individually
                for job in group_jobs:
                    if immediate:
                        self.run_job(job)
                    else:
                        self._job_submission_executor.submit(self.run_job, job)

    # ------------------------------------------------------------------
    # Single-job submission
    # ------------------------------------------------------------------

    # Resource keys that materially affect SGE scheduling.  Differences in
    # these across an array bucket are worth warning about; cosmetic
    # resources like 'name' are intentionally excluded.
    _ARRAY_RESOURCE_KEYS = (
        "mem_mb",
        "mem_mb_per_cpu",
        "runtime",
        "threads",
        "sge_queue",
        "sge_project",
        "sge_pe",
        "sge_resources",
    )

    def _warn_on_heterogeneous_resources(
        self, jobs: List[JobExecutorInterface]
    ) -> None:
        """Warn if jobs in an array bucket differ in scheduling resources.

        SGE applies one resource spec to every task in -t, so divergent
        per-task requirements would be silently flattened to the first
        job's values.
        """
        if len(jobs) < 2:
            return
        first = jobs[0]
        differing: dict = {}
        for key in self._ARRAY_RESOURCE_KEYS:
            ref = first.resources.get(key)
            for j in jobs[1:]:
                if j.resources.get(key) != ref:
                    differing.setdefault(key, set()).add(repr(ref))
                    differing[key].add(repr(j.resources.get(key)))
                    break
        if differing:
            summary = ", ".join(
                f"{k}={{{', '.join(sorted(v))}}}" for k, v in differing.items()
            )
            self.logger.warning(
                f"SGE array for rule '{first.name}' contains tasks with "
                f"differing resources ({summary}). The first task's values "
                f"will be applied to every task."
            )

    def _resolve_array_holds(
        self,
        chunk_jobs: List[JobExecutorInterface],
        chunk_start: int,
    ):
        """Decide whether to hold this array chunk with -hold_jid_ad or -hold_jid.

        Returns
        -------
        (hold_jid_ad, hold_jid_list)

        ``hold_jid_ad`` is the base SGE job ID of a single upstream array
        when every chunk task has exactly one upstream task and the
        upstream task index matches the downstream task index (the
        contract enforced by qsub's -hold_jid_ad).

        Otherwise ``hold_jid_ad`` is None and ``hold_jid_list`` carries
        all the upstream array job IDs to pass to plain -hold_jid (whole
        upstream array(s) must finish first).
        """
        # Collect each chunk task's upstreams.  Each entry is a list of
        # (sge_jobid, task_idx) tuples; task_idx is None when the
        # upstream was a single (non-array) submission.
        per_task: List[List[tuple]] = []
        all_base_ids: List[str] = []
        for j in chunk_jobs:
            entries: List[tuple] = []
            for _up, sge_jobid, task_idx in self._upstream_ext_ids(j):
                entries.append((sge_jobid, task_idx))
                if sge_jobid not in all_base_ids:
                    all_base_ids.append(sge_jobid)
            per_task.append(entries)

        if not all_base_ids:
            return (None, [])

        # -hold_jid_ad eligibility: every chunk task has exactly one
        # upstream, all upstreams share a single array job, and each
        # upstream task index equals the downstream task index.
        if len(all_base_ids) != 1:
            return (None, all_base_ids)
        upstream_base = all_base_ids[0]

        for offset, entries in enumerate(per_task):
            if len(entries) != 1:
                return (None, all_base_ids)
            sge_jobid, task_idx = entries[0]
            if task_idx is None:
                # Upstream was a single (non-array) submission.
                # -hold_jid_ad needs an array on both sides.
                return (None, all_base_ids)
            if sge_jobid != upstream_base:
                return (None, all_base_ids)
            if task_idx != chunk_start + offset:
                return (None, all_base_ids)

        self.logger.debug(
            f"Array chunk eligible for -hold_jid_ad on {upstream_base}"
        )
        return (upstream_base, [])

    def _upstream_ext_ids(self, job):
        """Yield ``(upstream_job, sge_jobid, task_idx)`` for each upstream.

        Reads from our authoritative in-memory map.  ``task_idx`` is
        ``None`` if the upstream was a single (non-array) submission.
        """
        try:
            dag_deps = self.workflow.dag.dependencies.get(job, {})
        except Exception as exc:
            self.logger.debug(
                f"Could not read DAG dependencies for job {job.jobid}: {exc}"
            )
            return
        for upstream_job in dag_deps:
            entry = self._job_to_sge.get(upstream_job)
            if entry is None:
                # Upstream hasn't been submitted yet (shouldn't happen
                # under --immediate-submit since Snakemake walks the DAG
                # in topological order); skip silently.
                continue
            sge_jobid, task_idx = entry
            yield upstream_job, sge_jobid, task_idx

    def _resolve_sge_dependencies(self, job) -> List[str]:
        """Return a deduped list of upstream SGE base job IDs.

        Used for single-task -hold_jid submission.  Drops any per-task
        suffix so the dependent waits on the whole upstream (array or
        not).
        """
        dep_ids: List[str] = []
        for _, sge_jobid, _ in self._upstream_ext_ids(job):
            base_id = str(sge_jobid).split(".")[0]
            if base_id not in dep_ids:
                dep_ids.append(base_id)
        return dep_ids

    def run_job(self, job: JobExecutorInterface) -> None:
        """Submit a single job via qsub."""
        group_or_rule = f"group_{job.name}" if job.is_group() else f"rule_{job.name}"
        wildcard_str = _get_job_wildcards(job)

        logdir = self.sge_logdir / group_or_rule / wildcard_str
        logdir.mkdir(parents=True, exist_ok=True)

        # SGE uses separate stdout/stderr streams unless -j y is passed
        log_stdout = logdir / "$JOB_ID.o"
        log_stderr = logdir / "$JOB_ID.e"

        job_params = {
            "run_uuid": self.run_uuid,
            "log_stdout": log_stdout,
            "log_stderr": log_stderr,
            "workdir": self.workflow.workdir_init,
        }

        # Resolve upstream SGE job IDs for -hold_jid (needed for --immediate-submit)
        dep_ids = self._resolve_sge_dependencies(job)

        exec_job = self.format_job_exec(job)
        call = get_submit_command(
            job,
            job_params,
            settings=self.workflow.executor_settings,
            exec_cmd=exec_job,
            hold_jid_list=dep_ids,
        )

        self.logger.debug(f"qsub call: {call}")
        try:
            out = subprocess.check_output(
                call,
                shell=True,
                text=True,
                stderr=subprocess.STDOUT,
            ).strip()
        except subprocess.CalledProcessError as e:
            self._report_error_threadsafe(
                SubmittedJobInfo(job),
                f"SGE qsub failed: {e.output.strip()}\n  Command: {call}",
            )
            return

        # qsub output: "Your job 12345 (\"name\") has been submitted"
        sge_jobid = _parse_qsub_jobid(out)
        if sge_jobid is None:
            self._report_error_threadsafe(
                SubmittedJobInfo(job),
                f"Could not parse SGE job ID from qsub output: {out!r}",
            )
            return

        self.logger.info(
            f"Job {job.jobid} submitted as SGE job {sge_jobid} "
            f"(log: {logdir})"
        )
        self._submitted_job_ids.append(sge_jobid)
        # Record the job→SGE-id mapping BEFORE notifying Snakemake so any
        # downstream submission triggered by the report sees it.
        self._job_to_sge[job] = (sge_jobid, None)
        # Resolve the actual log path now that we have the job ID
        log_stdout_resolved = logdir / f"{sge_jobid}.o"
        log_stderr_resolved = logdir / f"{sge_jobid}.e"
        self._report_submission_threadsafe(
            SubmittedJobInfo(
                job,
                external_jobid=sge_jobid,
                aux={
                    "log_stdout": log_stdout_resolved,
                    "log_stderr": log_stderr_resolved,
                    "submit_time": time.time(),
                },
            )
        )

    # ------------------------------------------------------------------
    # Array-job submission (group jobs)
    # ------------------------------------------------------------------

    def run_array_job(self, jobs: List[JobExecutorInterface]) -> None:
        """Submit all tasks in *jobs* as a single SGE array job.

        Each task is encoded as a zlib-compressed, base64-encoded JSON
        entry so that the submission script can unpack and execute it
        based on ``$SGE_TASK_ID``.

        The approach is identical to the SLURM plugin's ``run_array_jobs``
        method, adapted for SGE's ``qsub -t <start>-<end>`` syntax.

        Both group jobs and same-rule regular-job buckets are supported.
        All tasks in a single submission share one set of SGE resources
        (queue, memory, runtime, etc.) taken from the first job in the
        bucket -- callers should bucket jobs whose resources are
        compatible (e.g. all instances of the same rule).
        """
        if not jobs:
            return

        group_or_rule = (
            f"group_{jobs[0].name}"
            if jobs[0].is_group()
            else f"rule_{jobs[0].name}"
        )

        logdir = self.sge_logdir / group_or_rule
        logdir.mkdir(parents=True, exist_ok=True)

        # Build the compressed task → command map
        # task IDs in SGE arrays start at 1
        task_map = {
            str(idx): base64.b64encode(
                zlib.compress(self.format_job_exec(job).encode("utf-8"), level=9)
            ).decode()
            for idx, job in enumerate(jobs, start=1)
        }

        # Serialise the map; it will be embedded in the shell script
        task_map_json = json.dumps(task_map)
        task_map_b64 = base64.b64encode(task_map_json.encode()).decode()

        # Manifest: human-readable record of which task ID maps to which
        # wildcards.  Aids debugging when scanning SGE log files.
        manifest = {
            str(idx): {
                "snakemake_jobid": getattr(job, "jobid", None),
                "wildcards": dict(job.wildcards) if getattr(job, "wildcards", None) else {},
                "is_group": job.is_group(),
            }
            for idx, job in enumerate(jobs, start=1)
        }
        manifest_path = logdir / "task_manifest.json"
        try:
            manifest_path.write_text(json.dumps(manifest, indent=2))
        except OSError as exc:
            self.logger.debug(f"Could not write task manifest {manifest_path}: {exc}")

        # SGE arrays share one resource spec across all tasks.  When the
        # bucket contains jobs whose resources differ (e.g. per-wildcard
        # mem_mb / runtime), the first job's values are applied to every
        # task.  Warn so users notice silent over/under-allocation.
        self._warn_on_heterogeneous_resources(jobs)

        settings = self.workflow.executor_settings
        array_limit = settings.array_limit
        n_tasks = len(jobs)

        for chunk_start in range(1, n_tasks + 1, array_limit):
            chunk_end = min(chunk_start + array_limit - 1, n_tasks)
            chunk_jobs = jobs[chunk_start - 1 : chunk_end]

            # Build the submission script
            # The script reads SGE_TASK_ID, extracts the matching command
            # from the task map, decompresses it, and executes it.
            kind = "group" if jobs[0].is_group() else "rule"
            # The TASK_MAP and _tid variables must be `export`-ed so the
            # python3 child process invoked below can read them via
            # os.environ — plain shell variables are not inherited by
            # subprocesses.  (Pre-0.5.0 these came in via qsub -V, which
            # is no longer the default.)
            script_lines = [
                "#!/bin/bash",
                "set -euo pipefail",
                f"# SGE array job for Snakemake {kind} '{jobs[0].name}'",
                f"# run_uuid={self.run_uuid}",
                "",
                "# Task map: base64(JSON({task_id: base64(zlib(cmd))}))",
                f"export TASK_MAP={shlex.quote(task_map_b64)}",
                "",
                "# Extract and run the command for this task",
                "export _tid=${SGE_TASK_ID}",
                "_cmd=$(",
                "  python3 - <<'PYEOF'",
                "import sys, base64, zlib, json, os",
                "task_map = json.loads(base64.b64decode(os.environ['TASK_MAP']))",
                "tid = str(os.environ['_tid'])",
                "cmd = zlib.decompress(base64.b64decode(task_map[tid])).decode()",
                "sys.stdout.write(cmd)",
                "PYEOF",
                ")",
                "eval \"$_cmd\"",
            ]

            script_content = "\n".join(script_lines)

            # Write the script to a temp file so we can pass it to qsub
            script_path = logdir / f"array_job_{chunk_start}_{chunk_end}.sh"
            script_path.write_text(script_content)
            script_path.chmod(0o755)

            # Build qsub flags for the array
            job_params = {
                "run_uuid": self.run_uuid,
                "log_stdout": logdir / "$JOB_ID.$TASK_ID.o",
                "log_stderr": logdir / "$JOB_ID.$TASK_ID.e",
                "workdir": self.workflow.workdir_init,
                "array_range": f"{chunk_start}-{chunk_end}",
                "task_map_b64": task_map_b64,
            }

            # Resolve cross-array dependencies.  Two cases:
            #
            #   1. Per-task 1:1 matching across a single upstream array
            #      (e.g. run_bamos_correction[N] -> run_bamos[N], one
            #      subject per task).  → use SGE's -hold_jid_ad so a
            #      downstream task starts the moment its specific
            #      upstream task finishes, instead of waiting for the
            #      whole upstream array.
            #
            #   2. Anything else (multiple upstream arrays, or task
            #      indices that don't line up).  → fall back to
            #      -hold_jid (whole-array hold).
            hold_ad_id, hold_ids = self._resolve_array_holds(
                chunk_jobs, chunk_start
            )

            call = get_submit_command(
                chunk_jobs[0],
                job_params,
                settings=settings,
                exec_cmd=None,  # command is in script
                script_path=str(script_path),
                is_array=True,
                hold_jid_list=hold_ids,
                hold_jid_ad_override=hold_ad_id,
            )

            self.logger.debug(f"qsub array call: {call}")
            try:
                out = subprocess.check_output(
                    call,
                    shell=True,
                    text=True,
                    stderr=subprocess.STDOUT,
                ).strip()
            except subprocess.CalledProcessError as e:
                error_msg = (
                    f"SGE qsub array submission failed "
                    f"(tasks {chunk_start}-{chunk_end}): "
                    f"{e.output.strip()}\n  Command: {call}"
                )
                self.logger.error(error_msg)
                for job in chunk_jobs:
                    self._report_error_threadsafe(
                        SubmittedJobInfo(job),
                        f"Part of failed array qsub submission "
                        f"(tasks {chunk_start}-{chunk_end}); see log.",
                    )
                continue

            sge_jobid = _parse_qsub_jobid(out)
            if sge_jobid is None:
                self.logger.error(
                    f"Could not parse SGE array job ID from: {out!r}"
                )
                for job in chunk_jobs:
                    self._report_error_threadsafe(
                        SubmittedJobInfo(job),
                        f"Could not parse SGE job ID from qsub output: {out!r}",
                    )
                continue

            self._submitted_job_ids.append(sge_jobid)
            hold_msg = ""
            if hold_ad_id:
                hold_msg = f" -hold_jid_ad {hold_ad_id}"
            elif hold_ids:
                hold_msg = f" -hold_jid {','.join(hold_ids)}"
            self.logger.info(
                f"Submitted SGE array job {sge_jobid} "
                f"for {kind} '{jobs[0].name}' "
                f"(tasks {chunk_start}-{chunk_end}){hold_msg}."
            )

            # Record the job→SGE-id mapping for the whole chunk BEFORE
            # notifying Snakemake.  Each report_job_submission may unblock
            # the scheduler, which can immediately call run_jobs again
            # with downstream tasks that need to read these mappings.
            for task_idx, job in enumerate(chunk_jobs, start=chunk_start):
                self._job_to_sge[job] = (sge_jobid, task_idx)

            # Register each task with Snakemake
            for task_idx, job in enumerate(chunk_jobs, start=chunk_start):
                external_id = f"{sge_jobid}.{task_idx}"
                log_o = logdir / f"{sge_jobid}.{task_idx}.o"
                log_e = logdir / f"{sge_jobid}.{task_idx}.e"
                self._report_submission_threadsafe(
                    SubmittedJobInfo(
                        job,
                        external_jobid=external_id,
                        aux={
                            "log_stdout": log_o,
                            "log_stderr": log_e,
                            "submit_time": time.time(),
                        },
                    )
                )

    # ------------------------------------------------------------------
    # Status checking
    # ------------------------------------------------------------------

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> Generator[SubmittedJobInfo, None, None]:
        """Poll qstat / qacct to determine job completion status.

        Yields jobs that are still running/pending.
        Reports completed jobs via ``report_job_success``.
        Reports failed jobs via ``report_job_error``.
        """
        if not active_jobs:
            return

        settings = self.workflow.executor_settings
        max_sleep = 180
        initial_interval = settings.init_seconds_before_status_checks

        for attempt in range(settings.status_attempts):
            async with self.status_rate_limiter:
                status_map = await query_job_status(
                    active_jobs,
                    use_qacct=settings.use_qacct,
                    logger=self.logger,
                )
                if status_map is not None:
                    break
        else:
            # All attempts failed – yield all jobs as still running
            self.logger.warning(
                "All qstat/qacct status query attempts failed; "
                "treating all active jobs as still running."
            )
            for j in active_jobs:
                yield j
            return

        any_finished = False
        self.logger.debug(
            f"check_active_jobs: {len(active_jobs)} active, "
            f"status_map keys={list(status_map.keys())}, "
            f"values={list(status_map.values())}"
        )
        for j in active_jobs:
            status = status_map.get(j.external_jobid)
            submit_t = j.aux.get("submit_time", "N/A") if j.aux else "no-aux"
            self.logger.debug(
                f"  job {j.external_jobid}: status={status}, "
                f"submit_time={submit_t}, aux_keys={list(j.aux.keys()) if j.aux else None}"
            )

            if status is None:
                # Job not yet visible to qstat/qacct — assume still queued
                yield j
                continue

            if status == "finished":
                self.report_job_success(j)
                any_finished = True
                if not settings.keep_successful_logs:
                    self._delete_job_logs(j)
            elif status == "failed":
                log_files = [
                    str(j.aux.get("log_stdout", "")),
                    str(j.aux.get("log_stderr", "")),
                ]
                self.report_job_error(
                    j,
                    msg=(
                        f"SGE job '{j.external_jobid}' failed. "
                        f"Check logs: {log_files}"
                    ),
                    aux_logs=[lf for lf in log_files if lf],
                )
            else:
                # running / pending
                yield j

        if not any_finished:
            self.next_seconds_between_status_checks = min(
                self.next_seconds_between_status_checks + 10,
                max_sleep,
            )
        else:
            self.next_seconds_between_status_checks = initial_interval

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]) -> None:
        """Cancel all active SGE jobs via qdel."""
        cancel_sge_jobs(active_jobs, self.logger)

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def shutdown(self) -> None:
        self._job_submission_executor.shutdown(wait=True)
        super().shutdown()
        self.clean_old_logs()

    def clean_old_logs(self) -> None:
        """Delete log files older than *delete_logfiles_older_than* days."""
        age_cutoff = self.workflow.executor_settings.delete_logfiles_older_than
        if age_cutoff <= 0:
            return
        if self.workflow.executor_settings.keep_successful_logs:
            return
        cutoff_secs = age_cutoff * 86400
        now = time.time()
        self.logger.debug(
            f"Cleaning SGE log files older than {age_cutoff} day(s)."
        )
        for path in self.sge_logdir.rglob("*"):
            if path.is_file():
                try:
                    if now - path.stat().st_mtime > cutoff_secs:
                        path.unlink()
                except OSError as exc:
                    self.logger.warning(f"Could not delete log {path}: {exc}")
        # Clean up empty directories
        for path in sorted(self.sge_logdir.rglob("*"), reverse=True):
            if path.is_dir():
                try:
                    path.rmdir()  # Only removes if empty
                except OSError:
                    pass

    def _delete_job_logs(self, job_info: SubmittedJobInfo) -> None:
        """Delete stdout/stderr log files for a completed job."""
        for key in ("log_stdout", "log_stderr"):
            log_path = job_info.aux.get(key)
            if log_path and Path(log_path).exists():
                try:
                    Path(log_path).unlink()
                except OSError as exc:
                    self.logger.warning(
                        f"Could not delete log {log_path}: {exc}"
                    )

    # ------------------------------------------------------------------
    # Additional args passed to exec_job
    # ------------------------------------------------------------------

    def additional_general_args(self) -> str:
        """Extra Snakemake arguments forwarded to job-step execution."""
        return "--executor local --jobs 1"


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _parse_qsub_jobid(output: str) -> Optional[str]:
    """Extract the numeric job ID from qsub's output.

    Handles the common SGE/UGE variants::

        Your job 12345 ("name") has been submitted
        Your job-array 12345.1-10:1 ("name") has been submitted
        12345
    """
    # Try the standard verbose form first
    m = re.search(r"Your job(?:-array)?\s+(\d+)[.\s]", output)
    if m:
        return m.group(1)
    # Some clusters just emit the job ID on stdout
    m = re.match(r"^(\d+)$", output.strip())
    if m:
        return m.group(1)
    return None
