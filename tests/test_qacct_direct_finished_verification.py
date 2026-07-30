"""Verify that even a DIRECT qacct "finished" report (not just the
fallback-timeout paths) is cross-checked against the job's real declared
output before being trusted.

Root cause this guards against: reproduced live on a real ADNI3 test run.
A z-score job's inner shell command genuinely failed (non-zero exit, "Error
in rule z_score_z_score", zero output files ever created), yet qacct
reported it "finished" -- the failure occurred inside a nested
sub-invocation whose exit code apparently never propagated to what qacct
recorded for the outer SGE task. This is a scheduler/exit-code plumbing gap
outside this plugin's control, but the plugin can still catch it by not
blindly trusting *any* "finished" report -- confirming against the job's
own declared output closes the gap regardless of which code path produced
the (possibly wrong) "finished" signal.
"""
import asyncio
import os
import tempfile
import time
import types

from snakemake_executor_plugin_sge.job_status_query import query_job_status
import snakemake_executor_plugin_sge.job_status_query as jsq


def _fake_logger():
    return types.SimpleNamespace(warning=lambda *a, **k: None, debug=lambda *a, **k: None)


def _job_with_outputs(jobid, outputs, aux):
    fake_rule_job = types.SimpleNamespace(output=outputs)
    return types.SimpleNamespace(external_jobid=jobid, aux=aux, job=fake_rule_job)


def test_qacct_says_finished_but_output_missing_is_downgraded_to_failed(monkeypatch):
    """The exact real-world case: qacct itself (not the escalation
    fallback) reports 'finished', but the job's declared output was never
    created -- must be downgraded to failed."""
    monkeypatch.setattr(jsq, "_poll_qstat", lambda job_ids, logger: {})
    monkeypatch.setattr(jsq, "is_qacct_available", lambda: True)
    monkeypatch.setattr(jsq, "_poll_qacct", lambda jid, logger: "finished")

    with tempfile.TemporaryDirectory() as d:
        missing_output = os.path.join(d, "metric_z_score.nii.gz")
        aux = {"submit_time": time.time() - 30}
        j = _job_with_outputs("J1", [missing_output], aux)

        result = asyncio.run(query_job_status([j], use_qacct=True, logger=_fake_logger()))
        assert result == {"J1": "failed"}, (
            "qacct reporting 'finished' must not be trusted when the job's "
            "own declared output was never created"
        )


def test_qacct_says_finished_and_output_present_stays_finished(monkeypatch):
    """A genuine success (qacct says finished, output really exists) must
    not be second-guessed into a false failure."""
    monkeypatch.setattr(jsq, "_poll_qstat", lambda job_ids, logger: {})
    monkeypatch.setattr(jsq, "is_qacct_available", lambda: True)
    monkeypatch.setattr(jsq, "_poll_qacct", lambda jid, logger: "finished")

    with tempfile.TemporaryDirectory() as d:
        real_output = os.path.join(d, "metric_z_score.nii.gz")
        with open(real_output, "w") as f:
            f.write("real data")
        aux = {"submit_time": time.time() - 30}
        j = _job_with_outputs("J1", [real_output], aux)

        result = asyncio.run(query_job_status([j], use_qacct=True, logger=_fake_logger()))
        assert result == {"J1": "finished"}


def test_qacct_says_failed_directly_still_reported_failed(monkeypatch):
    """A direct qacct 'failed' report must pass through unchanged -- this
    new check only applies to 'finished', not to 'failed'."""
    monkeypatch.setattr(jsq, "_poll_qstat", lambda job_ids, logger: {})
    monkeypatch.setattr(jsq, "is_qacct_available", lambda: True)
    monkeypatch.setattr(jsq, "_poll_qacct", lambda jid, logger: "failed")

    aux = {"submit_time": time.time() - 30}
    j = types.SimpleNamespace(external_jobid="J1", aux=aux)  # no .job -- must not be needed

    result = asyncio.run(query_job_status([j], use_qacct=True, logger=_fake_logger()))
    assert result == {"J1": "failed"}
