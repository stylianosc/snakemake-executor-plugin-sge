"""Unit tests for _parse_qsub_jobid."""

from snakemake_executor_plugin_sge import _parse_qsub_jobid


def test_standard_output():
    out = 'Your job 54321 ("sm_abc") has been submitted'
    assert _parse_qsub_jobid(out) == "54321"


def test_array_output():
    out = 'Your job-array 99999.1-50:1 ("sm_abc") has been submitted'
    assert _parse_qsub_jobid(out) == "99999"


def test_bare_id():
    assert _parse_qsub_jobid("12345") == "12345"


def test_unrecognised_output():
    assert _parse_qsub_jobid("ERROR: no submit host") is None
