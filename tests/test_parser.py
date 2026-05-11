"""Unit tests for qsub job-ID parsing and qstat XML parsing."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from snakemake_executor_plugin_sge           import _parse_qsub_jobid
from snakemake_executor_plugin_sge.job_status_query import (
    _parse_qstat_xml,
    _expand_task_range,
    _state_is_error,
)

def test_standard_output():
    assert _parse_qsub_jobid('Your job 54321 ("sm_abc") has been submitted') == "54321"

def test_array_output():
    assert _parse_qsub_jobid('Your job-array 99999.1-50:1 ("sm_abc") has been submitted') == "99999"

def test_bare_id():
    assert _parse_qsub_jobid("12345") == "12345"

def test_unrecognised():
    assert _parse_qsub_jobid("ERROR: no submit host") is None

def test_expand_simple_range():
    assert _expand_task_range("1-5") == [1, 2, 3, 4, 5]

def test_expand_range_with_step():
    assert _expand_task_range("1-10:2") == [1, 3, 5, 7, 9]

def test_expand_single():
    assert _expand_task_range("7") == [7]

def test_expand_comma_list():
    assert set(_expand_task_range("2,4,6")) == {2, 4, 6}

def test_error_state_eqw():    assert _state_is_error("Eqw")
def test_error_state_d():      assert _state_is_error("d")
def test_running_state():      assert not _state_is_error("r")
def test_queued_state():       assert not _state_is_error("qw")
def test_transfer_state():     assert not _state_is_error("t")

QSTAT_XML = """<?xml version='1.0'?>
<job_info>
  <queue_info>
    <job_list state=\"running\">
      <JB_job_number>1001</JB_job_number>
      <state>r</state>
      <tasks>1-3</tasks>
    </job_list>
    <job_list state=\"running\">
      <JB_job_number>1002</JB_job_number>
      <state>r</state>
    </job_list>
  </queue_info>
  <job_info>
    <job_list state=\"pending\">
      <JB_job_number>1003</JB_job_number>
      <state>qw</state>
      <tasks>1-5:1</tasks>
    </job_list>
    <job_list state=\"pending\">
      <JB_job_number>1004</JB_job_number>
      <state>Eqw</state>
    </job_list>
  </job_info>
</job_info>
"""

def test_running_array_task():
    result = _parse_qstat_xml(QSTAT_XML, ["1001.2"])
    assert result["1001.2"] == "running"

def test_array_task_out_of_range():
    result = _parse_qstat_xml(QSTAT_XML, ["1001.9"])
    assert "1001.9" not in result

def test_non_array_job():
    result = _parse_qstat_xml(QSTAT_XML, ["1002"])
    assert result["1002"] == "running"

def test_pending_array_task():
    result = _parse_qstat_xml(QSTAT_XML, ["1003.3"])
    assert result["1003.3"] == "running"

def test_error_state_detection():
    result = _parse_qstat_xml(QSTAT_XML, ["1004"])
    assert result["1004"] == "failed"

def test_absent_job():
    result = _parse_qstat_xml(QSTAT_XML, ["9999"])
    assert "9999" not in result

def test_multiple_jobs_at_once():
    result = _parse_qstat_xml(QSTAT_XML, ["1001.1", "1002", "9999", "1004"])
    assert result["1001.1"] == "running"
    assert result["1002"]   == "running"
    assert result["1004"]   == "failed"
    assert "9999" not in result
