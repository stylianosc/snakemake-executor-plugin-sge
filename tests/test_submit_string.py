"""Unit tests for submit_string – no SGE cluster required."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from typing import Any

from snakemake_executor_plugin_sge.submit_string import (
    get_submit_command,
    _fmt_runtime,
    _fmt_mem,
)


class FakeResources:
    def __init__(self, **kw):
        self._d = kw
    def get(self, key, default=None):
        return self._d.get(key, default)

class FakeJob:
    def __init__(self, **kw):
        self.name = kw.pop("name", "test_rule")
        self.resources = FakeResources(**kw)
    def is_group(self): return False

class FakeSettings:
    def __init__(self):
        self.queue: Any = None
        self.pe: Any = None
        self.project: Any = None
        self.priority: Any = None
        self.export_env = True
        self.extra_envvars: Any = None
        self.requeue: Any = None
        self.reservation: Any = None
        self.notify = False
        self.mail_on: Any = None
        self.mail_address: Any = None
        self.hold_jid: Any = None
        self.task_concurrency: Any = None
        self.extra: Any = None
        self.join_logs = False

PARAMS = dict(run_uuid="abc123", log_dir="logs", workdir="")

def test_fmt_runtime_whole_hours():   assert _fmt_runtime(120) == "02:00:00"
def test_fmt_runtime_mixed():         assert _fmt_runtime(75)  == "01:15:00"
def test_fmt_runtime_sub_hour():      assert _fmt_runtime(30)  == "00:30:00"
def test_fmt_runtime_zero():          assert _fmt_runtime(0)   == "00:00:00"

def test_fmt_mem_round_gb():          assert _fmt_mem(4096) == "4G"
def test_fmt_mem_mb():                assert _fmt_mem(512)  == "512M"
def test_fmt_mem_non_round():         assert _fmt_mem(3000) == "3000M"
def test_fmt_mem_1gb():               assert _fmt_mem(1024) == "1G"

def test_basic():
    cmd = get_submit_command(FakeJob(), PARAMS, FakeSettings(), exec_cmd="/s.sh", is_array=True)
    assert "qsub" in cmd
    assert "-S /bin/bash" in cmd
    assert "-N test_rule_abc123" in cmd
    # Note: when is_array is True, the function tries to parse params["array_range"] which is not in PARAMS, defaulting to "1-1"
    assert "-t 1-1" in cmd

def test_log_dir_is_created(monkeypatch, tmp_path):
    created = {}

    def fake_mkdir(self, parents=False, exist_ok=False):
        created["path"] = str(self)
        created["parents"] = parents
        created["exist_ok"] = exist_ok

    monkeypatch.setattr("pathlib.Path.mkdir", fake_mkdir)

    params = dict(PARAMS, log_dir=str(tmp_path / "logs"))
    cmd = get_submit_command(FakeJob(), params, FakeSettings(), exec_cmd="/s.sh", is_array=True)
    assert "qsub" in cmd
    assert created == {"path": str(tmp_path / "logs"), "parents": True, "exist_ok": True}

def test_workdir_is_created(monkeypatch, tmp_path):
    created = []

    def fake_mkdir(self, parents=False, exist_ok=False):
        created.append({
            "path": str(self),
            "parents": parents,
            "exist_ok": exist_ok,
        })

    monkeypatch.setattr("pathlib.Path.mkdir", fake_mkdir)

    params = dict(PARAMS, log_dir=str(tmp_path / "logs"), workdir=str(tmp_path / "work"))
    cmd = get_submit_command(FakeJob(), params, FakeSettings(), exec_cmd="/s.sh", is_array=True)
    assert "qsub" in cmd
    assert {"path": str(tmp_path / "work"), "parents": True, "exist_ok": True} in created

def test_array_range():
    p = dict(PARAMS, array_range="3-20")
    cmd = get_submit_command(FakeJob(), p, FakeSettings(), exec_cmd="/s.sh", is_array=True)
    assert "-t 3-20" in cmd

def test_queue_from_settings():
    s = FakeSettings(); s.queue = "short.q"
    cmd = get_submit_command(FakeJob(), PARAMS, s, exec_cmd="/s.sh")
    assert "-q short.q" in cmd

def test_queue_override_per_rule():
    cmd = get_submit_command(
        FakeJob(sge_queue="highmem.q"), PARAMS, FakeSettings(), exec_cmd="/s.sh"
    )
    assert "-q highmem.q" in cmd

def test_pe_multithreaded():
    s = FakeSettings(); s.pe = "smp"
    cmd = get_submit_command(FakeJob(threads=8), PARAMS, s, "/s.sh", "1-1")
    assert "-pe smp 8" in cmd

def test_no_pe_multithreaded_falls_back_to_slots():
    cmd = get_submit_command(FakeJob(threads=4), PARAMS, FakeSettings(), "/s.sh", "1-1")
    assert "-l slots=4" in cmd

def test_runtime():
    cmd = get_submit_command(FakeJob(runtime=90), PARAMS, FakeSettings(), "/s.sh", "1-1")
    assert "-l h_rt=01:30:00" in cmd

def test_mem_mb_single_thread():
    cmd = get_submit_command(FakeJob(mem_mb=4096, threads=1), PARAMS, FakeSettings(), "/s.sh", "1-1")
    assert "h_vmem=4G" in cmd

def test_mem_mb_multithreaded_divides():
    cmd = get_submit_command(FakeJob(mem_mb=8192, threads=4), PARAMS, FakeSettings(), "/s.sh", "1-1")
    assert "h_vmem=2G" in cmd

def test_mem_mb_per_cpu():
    cmd = get_submit_command(FakeJob(mem_mb_per_cpu=1024), PARAMS, FakeSettings(), "/s.sh", "1-1")
    assert "h_vmem=1G" in cmd

def test_project():
    s = FakeSettings(); s.project = "myproj"
    cmd = get_submit_command(FakeJob(), PARAMS, s, "/s.sh", "1-1")
    assert "-P myproj" in cmd

def test_requeue_y():
    s = FakeSettings(); s.requeue = True
    cmd = get_submit_command(FakeJob(), PARAMS, s, "/s.sh", "1-1")
    assert "-r y" in cmd

def test_requeue_n():
    s = FakeSettings(); s.requeue = False
    cmd = get_submit_command(FakeJob(), PARAMS, s, "/s.sh", "1-1")
    assert "-r n" in cmd

def test_notify():
    s = FakeSettings(); s.notify = True
    cmd = get_submit_command(FakeJob(), PARAMS, s, "/s.sh", "1-1")
    assert "-notify" in cmd

def test_mail():
    s = FakeSettings(); s.mail_on = "be"; s.mail_address = "x@y.com"
    cmd = get_submit_command(FakeJob(), PARAMS, s, "/s.sh", "1-1")
    assert "-m be" in cmd and "-M x@y.com" in cmd

def test_hold_jid():
    s = FakeSettings(); s.hold_jid = "9999"
    cmd = get_submit_command(FakeJob(), PARAMS, s, "/s.sh", "1-1")
    assert "-hold_jid 9999" in cmd

def test_task_concurrency():
    s = FakeSettings(); s.task_concurrency = 10
    cmd = get_submit_command(FakeJob(), PARAMS, s, "/s.sh", "1-1")
    assert "-tc 10" in cmd

def test_sge_extra_passthrough():
    cmd = get_submit_command(FakeJob(sge_extra="-l gpu=1"), PARAMS, FakeSettings(), "/s.sh", "1-1")
    assert "-l gpu=1" in cmd

def test_global_extra_passthrough():
    s = FakeSettings(); s.extra = "-l special=yes"
    cmd = get_submit_command(FakeJob(), PARAMS, s, "/s.sh", "1-1")
    assert "-l special=yes" in cmd

def test_join_logs():
    s = FakeSettings(); s.join_logs = True
    cmd = get_submit_command(FakeJob(), PARAMS, s, "/s.sh", "1-1")
    assert "-j y" in cmd
    assert "-e " not in cmd

def test_sge_resources_dict():
    cmd = get_submit_command(
        FakeJob(sge_resources={"h_cpu": "24:00:00", "arch": "lx-amd64"}),
        PARAMS, FakeSettings(), "/s.sh", "1-1"
    )
    assert "-l h_cpu=24:00:00" in cmd
    assert "-l arch=lx-amd64" in cmd

def test_sge_resources_string():
    cmd = get_submit_command(
        FakeJob(sge_resources="h_cpu=24:00:00,arch=lx-amd64"),
        PARAMS, FakeSettings(), "/s.sh", "1-1"
    )
    assert "-l h_cpu=24:00:00" in cmd

def test_no_v_when_export_env_false():
    s = FakeSettings(); s.export_env = False
    cmd = get_submit_command(FakeJob(), PARAMS, s, "/s.sh", "1-1")
    assert " -V" not in cmd

def test_workdir(tmp_path):
    workdir = tmp_path / "work"
    params = dict(PARAMS, workdir=str(workdir))
    cmd = get_submit_command(FakeJob(), params, FakeSettings(), "/s.sh", "1-1")
    assert f"-wd {workdir}" in cmd

def test_no_workdir_uses_cwd():
    p = dict(PARAMS, workdir="")
    cmd = get_submit_command(FakeJob(), p, FakeSettings(), "/s.sh", "1-1")
    assert "-cwd" in cmd
