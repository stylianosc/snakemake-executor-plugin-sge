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
from typing import Dict, Generator, List, Optional, Tuple
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

        self.sge_logdir_default = _resolve_logdir(self.workflow)
        self.sge_logdir_default.mkdir(parents=True, exist_ok=True)

        self._job_submission_executor = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="sge_job_submit"
        )
        self._main_event_loop: Optional[asyncio.AbstractEventLoop] = None

        # Track submitted job IDs for cancellation
        self._submitted_job_ids: List[str] = []

        # Authoritative mapping from a Snakemake job to its SGE submission.
        # Each entry is (sge_jobid, task_idx) where task_idx is None for
        # non-array (single qsub) submissions, or the global subject index
        # for array submissions.  The global subject index is shared across
        # all rules: jobs that process the same wildcard combination always
        # get the same index, so -hold_jid_ad aligns downstream task N with
        # the correct upstream task N regardless of which rule or wave the
        # upstream was submitted in.
        self._job_to_sge: "dict[JobExecutorInterface, tuple]" = {}

        # Global subject index registry.  Assigns a unique, stable integer
        # index to each distinct wildcard combination seen across all rules.
        # This index is used as the SGE array task ID so that SGE's
        # -hold_jid_ad can align tasks purely by subject identity.
        self._subject_to_idx: Dict[str, int] = {}
        self._next_subject_idx: int = 0

        # Per-rule chunk counter used only for unique file naming.
        # (Replaces the old _rule_wave_num / _rule_task_end pair, which also
        # drove task-ID sequencing — that role is now handled by the global
        # subject index above.)
        self._rule_chunk_num: "dict[str, int]" = {}

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

        Regular (non-group) jobs are bucketed by rule name and submitted as
        SGE array jobs.  When Snakemake calls run_jobs() multiple times for
        the same rule (batching behaviour under --immediate-submit), each
        call produces a separate array submission (a "wave").  Task IDs
        continue globally across waves so that downstream rules can use
        -hold_jid_ad for per-subject parallelism regardless of wave boundaries.
        """
        if self._main_event_loop is None:
            try:
                self._main_event_loop = asyncio.get_running_loop()
            except RuntimeError:
                self._main_event_loop = None

        immediate = self.workflow.remote_execution_settings.immediate_submit

        group_jobs: List[JobExecutorInterface] = []
        regular_buckets: "dict[str, List[JobExecutorInterface]]" = {}
        for job in jobs:
            if job.is_group():
                group_jobs.append(job)
            else:
                regular_buckets.setdefault(job.name, []).append(job)

        for bucket in regular_buckets.values():
            bucket.sort(key=_wildcard_sort_key)

        for bucket in regular_buckets.values():
            if immediate:
                self.run_array_job(bucket)
            else:
                self._job_submission_executor.submit(self.run_array_job, bucket)

        if group_jobs:
            settings = self.workflow.executor_settings
            if settings.group_jobs_as_array and len(group_jobs) > 1:
                if immediate:
                    self.run_array_job(group_jobs)
                else:
                    self._job_submission_executor.submit(
                        self.run_array_job, group_jobs
                    )
            else:
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

    def _wildcard_key(self, job) -> str:
        """Canonical string key for the subject represented by *job*.

        Two jobs from different rules processing the same wildcard combination
        return the same key, so they get the same global subject index and
        therefore the same SGE task ID — a prerequisite for -hold_jid_ad.
        """
        wc = getattr(job, "wildcards", None)
        if not wc:
            return f"__jobid_{getattr(job, 'jobid', id(job))}"
        return "|".join(f"{k}={v}" for k, v in sorted(wc.items()))

    def _get_or_assign_subject_idx(self, job) -> int:
        """Return the global subject index for *job*, assigning one if new."""
        key = self._wildcard_key(job)
        if key not in self._subject_to_idx:
            self._next_subject_idx += 1
            self._subject_to_idx[key] = self._next_subject_idx
        return self._subject_to_idx[key]

    @staticmethod
    def _split_contiguous_ranges(
        ids: List[int],
    ) -> List[Tuple[int, int, List[int]]]:
        """Split a list of global subject indices into maximal contiguous runs.

        Returns a list of ``(range_start, range_end, ids_in_range)`` tuples.
        Used to work around the UCL cluster SGE/UGE restriction that the -t
        flag accepts only a single range specification — comma-separated
        multi-range specs (e.g. ``572-574,576-579``) are rejected with
        "qsub: -t option only allows one range specification".

        Each contiguous run is submitted as a separate qsub array job with
        ``-t range_start-range_end``.  The global subject index is preserved
        as the SGE task ID within each sub-job, so ``-hold_jid_ad`` alignment
        between rules is maintained: downstream task N always refers to the
        same subject as upstream task N across all sub-jobs.
        """
        sorted_ids = sorted(set(ids))
        if not sorted_ids:
            return []
        ranges: List[Tuple[int, int, List[int]]] = []
        start = end = sorted_ids[0]
        run: List[int] = [sorted_ids[0]]
        for i in sorted_ids[1:]:
            if i == end + 1:
                end = i
                run.append(i)
            else:
                ranges.append((start, end, run))
                start = end = i
                run = [i]
        ranges.append((start, end, run))
        return ranges

    def _resolve_array_holds(
        self,
        chunk_jobs: List[JobExecutorInterface],
    ):
        """Resolve upstream SGE dependencies for an array chunk.

        Because every job uses a global subject index as its SGE task ID,
        downstream task N always corresponds to the same subject as upstream
        task N across every rule.  -hold_jid_ad is therefore always correct
        for array upstreams: if the upstream task doesn't exist for a subject
        (its outputs were already produced in a previous run), SGE releases
        the downstream task automatically.

        Non-array upstreams (single qsub calls, task_idx=None) still require
        a whole-job -hold_jid because they carry no per-task index.

        Returns
        -------
        (hold_jid_ad, hold_jid_list)
        """
        hold_jid_ad_ids: List[str] = []  # array upstreams  → -hold_jid_ad
        hold_jid_ids: List[str] = []     # non-array upstreams → -hold_jid

        for j in chunk_jobs:
            for _, sge_jobid, task_idx in self._upstream_ext_ids(j):
                if task_idx is None:
                    if sge_jobid not in hold_jid_ids:
                        hold_jid_ids.append(sge_jobid)
                else:
                    if sge_jobid not in hold_jid_ad_ids:
                        hold_jid_ad_ids.append(sge_jobid)

        hold_ad = ",".join(hold_jid_ad_ids) if hold_jid_ad_ids else None
        if hold_ad:
            self.logger.debug(f"Array chunk using -hold_jid_ad on {hold_ad}")
        return (hold_ad, hold_jid_ids)

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

    def _get_job_logdir(self, job: JobExecutorInterface) -> Path:
        """Get the SGE log directory for a job.

        If the job specifies a workdir resource, place logs in
        {workdir}/.snakemake/sge_logs. Otherwise use the default logdir.
        """
        workdir = job.resources.get("workdir") if hasattr(job, "resources") else None
        if workdir:
            return (Path(workdir) / ".snakemake" / "sge_logs").resolve()
        return self.sge_logdir_default

    def run_job(self, job: JobExecutorInterface) -> None:
        """Submit a single job via qsub."""
        # Determine job-specific log directory (uses workdir resource if available)
        job_logdir = self._get_job_logdir(job)
        job_logdir.mkdir(parents=True, exist_ok=True)

        # Log files are stored directly in job_logdir with job ID
        # stdout: $JOB_ID.log, stderr: $JOB_ID.error
        log_stdout = job_logdir / "$JOB_ID.log"
        log_stderr = job_logdir / "$JOB_ID.error"

        # Use job's workdir resource if available, else fall back to workflow workdir
        workdir = job.resources.get("workdir") if hasattr(job, "resources") else None
        workdir = workdir or self.workflow.workdir_init

        job_params = {
            "run_uuid": self.run_uuid,
            "log_stdout": log_stdout,
            "log_stderr": log_stderr,
            "workdir": workdir,
        }

        # Resolve upstream SGE job IDs for -hold_jid (needed for --immediate-submit)
        dep_ids = self._resolve_sge_dependencies(job)

        exec_job = self.format_job_exec(job)

        # Write a wrapper script rather than piping exec_job via echo|qsub.
        # Script-based submission is cleaner and avoids shell quoting edge-cases.
        single_meta_dir = job_logdir / ".meta"
        single_meta_dir.mkdir(parents=True, exist_ok=True)
        safe_name = re.sub(r"[^\w-]", "_", job.name)

        single_script_path = single_meta_dir / f"{safe_name}_{self.run_uuid[:8]}.sh"
        single_script_path.write_text("\n".join([
            "#!/bin/bash",
            "set -euo pipefail",
            f"# SGE single job for Snakemake rule '{job.name}'",
            f"# run_uuid={self.run_uuid}",
            "",
            exec_job,
        ]))
        single_script_path.chmod(0o755)

        call = get_submit_command(
            job,
            job_params,
            settings=self.workflow.executor_settings,
            exec_cmd=None,
            script_path=str(single_script_path),
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
            # Route qsub confirmation through Snakemake's logger so it lands
            # in the .snakemake log file and respects quiet/verbosity settings
            # rather than always printing to stdout.
            self.logger.info(out)
        except subprocess.CalledProcessError as e:
            err_msg = f"SGE qsub failed: {e.output.strip()}\n  Command: {call}"
            self.logger.error(err_msg)
            self._report_error_threadsafe(
                SubmittedJobInfo(job),
                err_msg,
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
            f"(log: {job_logdir})"
        )
        self._submitted_job_ids.append(sge_jobid)
        # Record the job→SGE-id mapping BEFORE notifying Snakemake so any
        # downstream submission triggered by the report sees it.
        self._job_to_sge[job] = (sge_jobid, None)
        # Resolve the actual log path now that we have the job ID
        log_stdout_resolved = job_logdir / f"{sge_jobid}.log"
        log_stderr_resolved = job_logdir / f"{sge_jobid}.error"
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
        """Submit all tasks in *jobs* as one or more SGE array jobs.

        Each task is encoded as a zlib-compressed, base64-encoded JSON entry
        so that the submission script can unpack and execute it based on
        ``$SGE_TASK_ID``.

        Task IDs are GLOBAL SUBJECT INDICES shared across all rules: the same
        wildcard combination always gets the same integer index regardless of
        which rule submits it.  This means downstream task N and upstream task
        N always refer to the same subject, so SGE's -hold_jid_ad gives exact
        per-subject dependency tracking with no eligibility check needed.

        When a subject's upstream outputs already exist from a previous run,
        that subject is not resubmitted for the upstream rule.  Its task ID
        therefore does not exist in the upstream SGE array, and SGE releases
        the downstream task automatically — no stale hold.
        """
        if not jobs:
            return

        group_or_rule = (
            f"group_{jobs[0].name}"
            if jobs[0].is_group()
            else f"rule_{jobs[0].name}"
        )

        first_job_logdir = self.sge_logdir_default
        first_job_logdir.mkdir(parents=True, exist_ok=True)

        meta_dir = first_job_logdir / ".meta" / group_or_rule
        meta_dir.mkdir(parents=True, exist_ok=True)

        # Assign each job its global subject index.  The same wildcard
        # combination always maps to the same index across all rules, so
        # -hold_jid_ad can align tasks purely by subject identity.
        subject_idxs = [self._get_or_assign_subject_idx(j) for j in jobs]

        # Build the compressed task → command map keyed by global subject index.
        # All chunks of this call share one map file so submission scripts
        # look up by $SGE_TASK_ID directly.
        task_map = {
            str(idx): base64.b64encode(
                zlib.compress(self.format_job_exec(job).encode("utf-8"), level=9)
            ).decode()
            for idx, job in zip(subject_idxs, jobs)
        }
        task_map_b64 = base64.b64encode(json.dumps(task_map).encode()).decode()

        # Chunk counter used only for unique file naming.
        rule_key = group_or_rule
        chunk_num = self._rule_chunk_num.get(rule_key, 0) + 1
        self._rule_chunk_num[rule_key] = chunk_num

        task_map_file = meta_dir / f"task_map_chunk{chunk_num}.b64"
        task_map_file.write_text(task_map_b64)

        # Human-readable manifest for debugging: global index → wildcards.
        manifest = {
            str(idx): {
                "snakemake_jobid": getattr(job, "jobid", None),
                "wildcards": dict(job.wildcards) if getattr(job, "wildcards", None) else {},
                "is_group": job.is_group(),
            }
            for idx, job in zip(subject_idxs, jobs)
        }
        manifest_path = meta_dir / f"task_manifest_chunk{chunk_num}.json"
        try:
            manifest_path.write_text(json.dumps(manifest, indent=2))
        except OSError as exc:
            self.logger.debug(f"Could not write task manifest {manifest_path}: {exc}")

        self._warn_on_heterogeneous_resources(jobs)

        settings = self.workflow.executor_settings
        array_limit = settings.array_limit
        kind = "group" if jobs[0].is_group() else "rule"
        workdir = str(self.workflow.workdir_init)

        # Submit in chunks of at most array_limit tasks each.  Within each
        # chunk, further split non-contiguous index sets into separate qsub
        # calls — this cluster's SGE/UGE rejects comma-separated multi-range
        # -t specifications with "qsub: -t option only allows one range
        # specification".  Each contiguous sub-range is its own array job,
        # which preserves the global subject index as the SGE task ID and
        # therefore keeps -hold_jid_ad alignment across rules intact.
        for sub_chunk, chunk_offset in enumerate(range(0, len(jobs), array_limit), start=1):
            chunk_jobs = jobs[chunk_offset:chunk_offset + array_limit]
            chunk_idxs = subject_idxs[chunk_offset:chunk_offset + array_limit]

            idx_to_job = dict(zip(chunk_idxs, chunk_jobs))
            sub_ranges = self._split_contiguous_ranges(chunk_idxs)

            for sub_range_num, (sub_start, sub_end, sub_idxs) in enumerate(sub_ranges, start=1):
                sub_jobs = [idx_to_job[i] for i in sub_idxs]

                # SGE -t accepts either a single ID or a start-end range.
                task_spec = (
                    str(sub_start)
                    if sub_start == sub_end
                    else f"{sub_start}-{sub_end}"
                )

                script_lines = [
                    "#!/bin/bash",
                    "set -euo pipefail",
                    f"# SGE array job for Snakemake {kind} '{jobs[0].name}'",
                    f"# run_uuid={self.run_uuid}",
                    "",
                    "# Read the task map from the shared filesystem file.",
                    "# Avoids ARG_MAX issues for large arrays (150+ tasks).",
                    f"export TASK_MAP_FILE={shlex.quote(str(task_map_file))}",
                    "",
                    "# Decode the exec command for this task from the task map.",
                    "# $SGE_TASK_ID is the global subject index for this task.",
                    "export _tid=${SGE_TASK_ID}",
                    "_cmd=$(",
                    "  python3 - <<'PYEOF'",
                    "import sys, base64, zlib, json, os",
                    "task_map = json.loads(base64.b64decode(open(os.environ['TASK_MAP_FILE']).read()))",
                    "tid = str(os.environ['_tid'])",
                    "cmd = zlib.decompress(base64.b64decode(task_map[tid])).decode()",
                    "sys.stdout.write(cmd)",
                    "PYEOF",
                    ")",
                    "",
                    "eval \"$_cmd\"",
                ]

                script_path = (
                    meta_dir / f"array_job_chunk{chunk_num}_{sub_chunk}_{sub_range_num}.sh"
                )
                script_path.write_text("\n".join(script_lines))
                script_path.chmod(0o755)

                job_params = {
                    "run_uuid": self.run_uuid,
                    "log_stdout": first_job_logdir / "$JOB_ID.$TASK_ID.log",
                    "log_stderr": first_job_logdir / "$JOB_ID.$TASK_ID.error",
                    "workdir": workdir,
                    "array_range": task_spec,
                }

                # Resolve holds per sub-range: each sub-job's upstream tasks
                # may have been submitted in a different upstream sub-range
                # job, so _resolve_array_holds returns the exact SGE job IDs
                # that cover the subjects in this sub-range.
                hold_ad_id, hold_ids = self._resolve_array_holds(sub_jobs)

                call = get_submit_command(
                    sub_jobs[0],
                    job_params,
                    settings=settings,
                    exec_cmd=None,
                    script_path=str(script_path),
                    is_array=True,
                    hold_jid_list=hold_ids,
                    hold_jid_ad_override=hold_ad_id,
                )

                self.logger.debug(f"qsub array call (sub-range {task_spec}): {call}")
                try:
                    out = subprocess.check_output(
                        call,
                        shell=True,
                        text=True,
                        stderr=subprocess.STDOUT,
                    ).strip()
                    self.logger.info(out)
                except subprocess.CalledProcessError as e:
                    error_msg = (
                        f"SGE qsub array submission failed (tasks {task_spec}): "
                        f"{e.output.strip()}\n  Command: {call}"
                    )
                    self.logger.error(error_msg)
                    for job in sub_jobs:
                        self._report_error_threadsafe(
                            SubmittedJobInfo(job),
                            f"Part of failed array qsub submission (tasks {task_spec}); see log.",
                        )
                    continue

                sge_jobid = _parse_qsub_jobid(out)
                if sge_jobid is None:
                    self.logger.error(f"Could not parse SGE array job ID from: {out!r}")
                    for job in sub_jobs:
                        self._report_error_threadsafe(
                            SubmittedJobInfo(job),
                            f"Could not parse SGE job ID from qsub output: {out!r}",
                        )
                    continue

                self._submitted_job_ids.append(sge_jobid)
                hold_msg = ""
                if hold_ad_id:
                    hold_msg += f" -hold_jid_ad {hold_ad_id}"
                if hold_ids:
                    hold_msg += f" -hold_jid {','.join(hold_ids)}"
                self.logger.info(
                    f"Submitted SGE array job {sge_jobid} "
                    f"for {kind} '{jobs[0].name}' "
                    f"(tasks {task_spec}){hold_msg}."
                )

                # Record in _job_to_sge BEFORE notifying Snakemake so
                # downstream hold resolution sees this sub-range's job ID.
                for idx, job in zip(sub_idxs, sub_jobs):
                    self._job_to_sge[job] = (sge_jobid, idx)

                for idx, job in zip(sub_idxs, sub_jobs):
                    external_id = f"{sge_jobid}.{idx}"
                    log_o = first_job_logdir / f"{sge_jobid}.{idx}.log"
                    log_e = first_job_logdir / f"{sge_jobid}.{idx}.error"
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

        for _ in range(settings.status_attempts):
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
        for path in self.sge_logdir_default.rglob("*"):
            if path.is_file():
                try:
                    if now - path.stat().st_mtime > cutoff_secs:
                        path.unlink()
                except OSError as exc:
                    self.logger.warning(f"Could not delete log {path}: {exc}")
        # Clean up empty directories
        for path in sorted(self.sge_logdir_default.rglob("*"), reverse=True):
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
