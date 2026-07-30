"""Verify that a job qacct can never confirm is checked against its own
declared output files before being trusted as "finished" -- not just
assumed to have succeeded.

Root cause this guards against: the qacct-unavailability fallback added
earlier (see test_qacct_permanent_unavailability.py) reported a real ADNI3
job ("dti_dti" for subj-006-s-6610) as "finished" purely because it had
left qstat and qacct could never confirm it -- but the job's declared
output (fa.nii.gz) was never actually created. The job had been silently
rejected/never truly executed, not successfully completed. A blind
"assume finished" masks exactly this class of failure instead of surfacing
it. Checking the job's own declared outputs on disk distinguishes
"genuinely succeeded, just untracked by a broken qacct" from "never
actually ran" -- the two cases a bare timeout can't tell apart.
"""
import asyncio
import os
import tempfile
import time
import types

from snakemake_executor_plugin_sge.job_status_query import query_job_status, _job_outputs_exist
import snakemake_executor_plugin_sge.job_status_query as jsq


def _fake_logger():
    return types.SimpleNamespace(warning=lambda *a, **k: None, debug=lambda *a, **k: None)


def _job_with_outputs(jobid, outputs, aux):
    """A SubmittedJobInfo-like object whose .job.output lists real paths."""
    fake_rule_job = types.SimpleNamespace(output=outputs)
    return types.SimpleNamespace(external_jobid=jobid, aux=aux, job=fake_rule_job)


# ---------------------------------------------------------------------------
# _job_outputs_exist unit tests
# ---------------------------------------------------------------------------
def test_job_outputs_exist_true_when_all_present(tmp_path):
    f = tmp_path / "out.csv"
    f.write_text("data")
    job_info = types.SimpleNamespace(job=types.SimpleNamespace(output=[str(f)]))
    assert _job_outputs_exist(job_info) is True


def test_job_outputs_exist_false_when_missing(tmp_path):
    missing = tmp_path / "never_written.csv"
    job_info = types.SimpleNamespace(job=types.SimpleNamespace(output=[str(missing)]))
    assert _job_outputs_exist(job_info) is False


def test_job_outputs_exist_none_when_uninspectable():
    job_info = types.SimpleNamespace()  # no .job attribute at all
    assert _job_outputs_exist(job_info) is None


def test_job_outputs_exist_none_when_no_outputs_declared():
    job_info = types.SimpleNamespace(job=types.SimpleNamespace(output=[]))
    assert _job_outputs_exist(job_info) is None


# ---------------------------------------------------------------------------
# Integrated behavior via query_job_status
# ---------------------------------------------------------------------------
def test_qacct_never_resolves_but_output_missing_reports_failed(monkeypatch):
    """The exact real-world case: job left qstat, qacct can never confirm
    it, and its declared output was never created -- must report FAILED,
    not silently FINISHED."""
    monkeypatch.setattr(jsq, "_poll_qstat", lambda job_ids, logger: {})
    monkeypatch.setattr(jsq, "is_qacct_available", lambda: True)
    monkeypatch.setattr(jsq, "_poll_qacct", lambda jid, logger: None)

    with tempfile.TemporaryDirectory() as d:
        missing_output = os.path.join(d, "fa.nii.gz")
        aux = {"submit_time": time.time() - 30, "first_qacct_miss": time.time() - 91}
        j = _job_with_outputs("J1", [missing_output], aux)

        result = asyncio.run(query_job_status([j], use_qacct=True, logger=_fake_logger()))
        assert result == {"J1": "failed"}, (
            "a job whose declared output was never created must be reported "
            "failed, not finished, even after the qacct grace period"
        )


def test_qacct_never_resolves_but_output_present_reports_finished(monkeypatch):
    """Same ambiguous qacct situation, but the output genuinely exists --
    must report finished (this is the legitimate 'qacct just can't confirm
    a real success' case)."""
    monkeypatch.setattr(jsq, "_poll_qstat", lambda job_ids, logger: {})
    monkeypatch.setattr(jsq, "is_qacct_available", lambda: True)
    monkeypatch.setattr(jsq, "_poll_qacct", lambda jid, logger: None)

    with tempfile.TemporaryDirectory() as d:
        real_output = os.path.join(d, "fa.nii.gz")
        with open(real_output, "w") as f:
            f.write("real data")
        aux = {"submit_time": time.time() - 30, "first_qacct_miss": time.time() - 91}
        j = _job_with_outputs("J1", [real_output], aux)

        result = asyncio.run(query_job_status([j], use_qacct=True, logger=_fake_logger()))
        assert result == {"J1": "finished"}


def test_qacct_disabled_outright_still_checks_output_before_reporting_finished(monkeypatch):
    """The pre-existing 'qacct unavailable' fallback must also verify
    output rather than blindly assuming success."""
    monkeypatch.setattr(jsq, "_poll_qstat", lambda job_ids, logger: {})

    with tempfile.TemporaryDirectory() as d:
        missing_output = os.path.join(d, "whole_brain_metrics.csv")
        aux = {"submit_time": time.time() - 30}
        j = _job_with_outputs("J1", [missing_output], aux)

        result = asyncio.run(query_job_status([j], use_qacct=False, logger=_fake_logger()))
        assert result == {"J1": "failed"}
