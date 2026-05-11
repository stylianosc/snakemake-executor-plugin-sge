"""Unit tests for submit_string.get_submit_command."""

import types
import pytest
from snakemake_executor_plugin_sge.submit_string import (
    get_submit_command,
    _fmt_runtime,
    _fmt_mem,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeResources:
    """Minimal stand-in for Snakemake's job.resources mapping."""
    def __init__(self, **kw):
        self._d = kw

    def get(self, key, default=None):
        return self._d.get(key, default)


class FakeJob:
    def __init__(self, **kw):
        self.name = kw.pop("name", "test_rule")
        self.resources = FakeResources(**kw)

    def is_group(self):
        return False


class FakeSettings:
    queue = None
    pe = None
    project = None


# ---------------------------------------------------------------------------
# _fmt_runtime
# ---------------------------------------------------------------------------

def test_fmt_runtime_whole_hours():
    assert _fmt_runtime(120) == "02:00:00"


def test_fmt_runtime_mixed():
    assert _fmt_runtime(75) == "01:15:00"


def test_fmt_runtime_less_than_hour():
    assert _fmt_runtime(30) == "00:30:00"


# ---------------------------------------------------------------------------
# _fmt_mem
# ---------------------------------------------------------------------------

def test_fmt_mem_gb():
    assert _fmt_mem(4096) == "4G"


def test_fmt_mem_mb():
    assert _fmt_mem(512) == "512M"


def test_fmt_mem_non_round_gb():
    # 3000 MB is not a round number of GB → should stay as MB
    assert _fmt_mem(3000) == "3000M"


# ---------------------------------------------------------------------------
# get_submit_command – basic
# ---------------------------------------------------------------------------

DEFAULT_PARAMS = {
    "run_uuid": "abc123",
    "log_stdout": "/logs/$JOB_ID.o",
    "log_stderr": "/logs/$JOB_ID.e",
    "workdir": "/work",
}


def test_basic_single_job():
    job = FakeJob(threads=1)
    cmd = get_submit_command(
        job, dict(DEFAULT_PARAMS), FakeSettings(), exec_cmd="echo hello"
    )
    assert "qsub" in cmd
    assert "-V" in cmd
    assert "-N" in cmd
    assert "echo hello" in cmd
    assert "-t " not in cmd  # not an array job


def test_queue_flag():
    settings = FakeSettings()
    settings.queue = "short.q"
    job = FakeJob(threads=1)
    cmd = get_submit_command(
        job, dict(DEFAULT_PARAMS), settings, exec_cmd="echo hi"
    )
    assert "-q short.q" in cmd


def test_pe_flag_multi_thread():
    settings = FakeSettings()
    settings.pe = "smp"
    job = FakeJob(threads=8)
    cmd = get_submit_command(
        job, dict(DEFAULT_PARAMS), settings, exec_cmd="echo hi"
    )
    assert "-pe smp 8" in cmd


def test_runtime_flag():
    job = FakeJob(threads=1, runtime=90)
    cmd = get_submit_command(
        job, dict(DEFAULT_PARAMS), FakeSettings(), exec_cmd="echo hi"
    )
    assert "-l h_rt=01:30:00" in cmd


def test_mem_mb_flag():
    job = FakeJob(threads=1, mem_mb=2048)
    cmd = get_submit_command(
        job, dict(DEFAULT_PARAMS), FakeSettings(), exec_cmd="echo hi"
    )
    assert "h_vmem=2G" in cmd


def test_array_flag():
    params = dict(DEFAULT_PARAMS)
    params["array_range"] = "1-10"
    params["task_map_b64"] = "dummyb64"
    job = FakeJob(threads=1)
    cmd = get_submit_command(
        job, params, FakeSettings(), exec_cmd=None,
        script_path="/tmp/array.sh", is_array=True,
    )
    assert "-t 1-10" in cmd
    assert "/tmp/array.sh" in cmd


def test_sge_extra_passthrough():
    job = FakeJob(threads=1, sge_extra="-l gpu=1")
    cmd = get_submit_command(
        job, dict(DEFAULT_PARAMS), FakeSettings(), exec_cmd="echo hi"
    )
    assert "-l gpu=1" in cmd
