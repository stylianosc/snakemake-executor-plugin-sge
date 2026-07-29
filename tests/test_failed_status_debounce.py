"""Verify check_active_jobs debounces "failed" status reports.

Root cause this guards against: on a real EPAD process-all run
(2026-07-28), SGE array task 7118778.101 (rule
metrics_extract_metrics_freesurfer) was reported "failed" by a single
qstat/qacct poll and Snakemake cancelled the entire remaining DAG (~680
steps) as a result -- but the task's actual output files
(whole_brain_metrics.csv etc.) were present, complete, and correctly
timestamped. The job had genuinely succeeded; the single status poll was a
transient false positive. check_active_jobs must not act on a "failed"
report until it has been observed on two consecutive independent polls.
"""

import asyncio
import types

import pytest

from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo

from snakemake_executor_plugin_sge import Executor
import snakemake_executor_plugin_sge as plugin_module


class FakeJob:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name


def _make_executor(status_sequence):
    """Bare Executor with just the state check_active_jobs touches.

    ``status_sequence`` is a list of dicts; each call to the (mocked)
    query_job_status pops the next dict off the front.
    """
    ex = Executor.__new__(Executor)
    ex.logger = types.SimpleNamespace(
        debug=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        info=lambda *a, **k: None,
    )
    ex.workflow = types.SimpleNamespace(
        executor_settings=types.SimpleNamespace(
            status_attempts=1,
            init_seconds_before_status_checks=5,
            use_qacct=True,
            keep_successful_logs=True,
        )
    )
    ex.status_rate_limiter = asyncio.Semaphore(1)
    ex.next_seconds_between_status_checks = 5
    ex._failed_confirm_counts = {}

    successes, errors = [], []
    ex.report_job_success = lambda j: successes.append(j.external_jobid)
    ex.report_job_error = lambda j, msg="", aux_logs=None: errors.append(
        (j.external_jobid, msg)
    )
    ex._delete_job_logs = lambda j: None

    remaining = list(status_sequence)

    async def fake_query_job_status(active_jobs, use_qacct, logger):
        return remaining.pop(0)

    return ex, successes, errors, fake_query_job_status


def _job(jobid):
    return SubmittedJobInfo(job=FakeJob(jobid), external_jobid=jobid, aux={})


async def _drain(ex, active_jobs):
    """Run check_active_jobs once, returning the jobs it yields (still active)."""
    return [j async for j in ex.check_active_jobs(active_jobs)]


def test_single_failed_report_is_not_acted_on(monkeypatch):
    """One "failed" poll alone must not call report_job_error."""
    ex, successes, errors, fake_query = _make_executor(
        [{"J1": "failed"}]
    )
    monkeypatch.setattr(plugin_module, "query_job_status", fake_query)

    j1 = _job("J1")
    still_active = asyncio.run(_drain(ex, [j1]))

    assert errors == [], "a single transient 'failed' poll must not be reported"
    assert successes == []
    assert still_active == [j1], "job must still be yielded as active/pending"
    assert ex._failed_confirm_counts.get("J1") == 1


def test_failed_confirmed_on_second_consecutive_poll(monkeypatch):
    """A genuinely, persistently failed job IS reported after 2 polls."""
    ex, successes, errors, fake_query = _make_executor(
        [{"J1": "failed"}, {"J1": "failed"}]
    )
    monkeypatch.setattr(plugin_module, "query_job_status", fake_query)

    j1 = _job("J1")
    asyncio.run(_drain(ex, [j1]))          # 1st poll: not yet reported
    still_active = asyncio.run(_drain(ex, [j1]))  # 2nd poll: confirmed

    assert len(errors) == 1
    assert errors[0][0] == "J1"
    assert still_active == [], "confirmed-failed job must not be yielded again"
    assert "J1" not in ex._failed_confirm_counts, "counter must be cleared after reporting"


def test_transient_failed_then_finished_reports_success_not_failure(monkeypatch):
    """The exact real-world case: 'failed' once, then 'finished' -- must
    report success, never failure."""
    ex, successes, errors, fake_query = _make_executor(
        [{"J1": "failed"}, {"J1": "finished"}]
    )
    monkeypatch.setattr(plugin_module, "query_job_status", fake_query)

    j1 = _job("J1")
    asyncio.run(_drain(ex, [j1]))                   # 1st poll: "failed", held back
    still_active = asyncio.run(_drain(ex, [j1]))    # 2nd poll: "finished"

    assert errors == [], "must never report a job that ultimately finished successfully"
    assert successes == ["J1"]
    assert still_active == []
    assert "J1" not in ex._failed_confirm_counts
