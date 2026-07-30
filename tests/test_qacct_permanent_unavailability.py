"""Verify query_job_status eventually resolves a job that left qstat when
qacct is nominally available (binary on PATH) but never actually records
the job -- e.g. because the cluster's accounting data store itself is
missing/broken, not just slow.

Root cause this guards against: on this cluster, `qacct -j <id>` was
observed to fail with "/opt/gridengine/default/common/accounting: No such
file or directory" -- meaning _poll_qacct returns None on *every* poll,
forever, for *every* job. Before this fix, a job that left qstat (finished
or failed) but was never recorded by qacct stayed "still queued" in
Snakemake's view permanently, since only the initial 20s youth grace period
was time-bounded -- there was no escalation once a job aged past that with
qacct still returning nothing. This silently stalled real DAG targets
(confirmed for real ADNI3/EPAD subjects whose stage was listed as a DAG
target, presumably ran, then vanished from the scheduler with no further
resolution) indefinitely, with no error and no explanation.
"""
import asyncio
import time
import types

from snakemake_executor_plugin_sge.job_status_query import query_job_status
import snakemake_executor_plugin_sge.job_status_query as jsq


def _job(jobid, submit_time, aux=None):
    return types.SimpleNamespace(
        external_jobid=jobid,
        aux=aux if aux is not None else {"submit_time": submit_time},
    )


def _fake_logger():
    return types.SimpleNamespace(warning=lambda *a, **k: None, debug=lambda *a, **k: None)


def test_still_young_job_missing_from_qstat_stays_unresolved(monkeypatch):
    """A job that just left qstat (< 20s old) must not be resolved yet --
    it may just not have appeared in qstat's output this instant."""
    monkeypatch.setattr(jsq, "_poll_qstat", lambda job_ids, logger: {})
    monkeypatch.setattr(jsq, "is_qacct_available", lambda: True)
    monkeypatch.setattr(jsq, "_poll_qacct", lambda jid, logger: None)

    j = _job("J1", submit_time=time.time())
    result = asyncio.run(query_job_status([j], use_qacct=True, logger=_fake_logger()))
    assert result == {}, "a job younger than the grace period must stay unresolved"


def test_qacct_permanently_missing_eventually_resolves_to_finished(monkeypatch):
    """qacct never finding the job (broken accounting store) must NOT leave
    the job stuck forever -- after the extra grace period, assume finished."""
    monkeypatch.setattr(jsq, "_poll_qstat", lambda job_ids, logger: {})
    monkeypatch.setattr(jsq, "is_qacct_available", lambda: True)
    monkeypatch.setattr(jsq, "_poll_qacct", lambda jid, logger: None)

    # Old enough to pass the initial 20s youth check.
    aux = {"submit_time": time.time() - 30}
    j = _job("J1", submit_time=None, aux=aux)

    # First poll past the youth window: qacct still returns None -> starts
    # the "first_qacct_miss" clock, must NOT resolve yet.
    result1 = asyncio.run(query_job_status([j], use_qacct=True, logger=_fake_logger()))
    assert result1 == {}, "first qacct miss must not immediately resolve the job"
    assert "first_qacct_miss" in j.aux

    # Simulate time passing past the 90s extra grace period without
    # mutating real time: backdate the recorded first-miss timestamp.
    j.aux["first_qacct_miss"] = time.time() - 91

    result2 = asyncio.run(query_job_status([j], use_qacct=True, logger=_fake_logger()))
    assert result2 == {"J1": "finished"}, (
        "after the extra grace period, a job qacct can never confirm must "
        "resolve to 'finished' rather than staying unresolved forever"
    )


def test_qacct_recovering_before_grace_period_clears_the_miss_clock(monkeypatch):
    """If qacct DOES find the job on a later poll, the miss-clock must reset
    -- confirms this isn't a one-shot latch that fires regardless."""
    monkeypatch.setattr(jsq, "_poll_qstat", lambda job_ids, logger: {})
    monkeypatch.setattr(jsq, "is_qacct_available", lambda: True)

    aux = {"submit_time": time.time() - 30, "first_qacct_miss": time.time() - 10}
    j = _job("J1", submit_time=None, aux=aux)

    monkeypatch.setattr(jsq, "_poll_qacct", lambda jid, logger: "finished")
    result = asyncio.run(query_job_status([j], use_qacct=True, logger=_fake_logger()))

    assert result == {"J1": "finished"}
    assert "first_qacct_miss" not in j.aux, "a successful qacct read must clear the miss clock"
