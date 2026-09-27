"""Files a run writes into the shared log/meta directories are unique per run.

Array tasks read their task map when they start, and the log/meta directories
are shared by every run in the same output directory. When two runs used the
same file names (task_map_chunk1.b64, ...), a later run silently replaced the
task maps of an earlier run's still-queued arrays (EPAD, 2026-09-27). Every such
name now goes through Executor._chunk_stem(), which carries the run's tag.
"""
import logging
import types

import pytest

import snakemake_executor_plugin_sge as plugin
from snakemake_executor_plugin_sge import Executor


def _new_executor(monkeypatch, tmp_path, prefix=""):
    """An Executor through its real __post_init__, minus SGE and atexit."""
    monkeypatch.setattr(plugin, "is_qstat_available", lambda: True)
    monkeypatch.setattr(plugin, "_resolve_logdir", lambda workflow: tmp_path / "sge_logs")
    monkeypatch.setattr(plugin.atexit, "register", lambda *a, **k: None)
    ex = Executor.__new__(Executor)
    ex.logger = logging.getLogger("test")
    ex.logger.addHandler(logging.NullHandler())
    ex.workflow = types.SimpleNamespace(
        executor_settings=types.SimpleNamespace(jobname_prefix=prefix))
    ex.__post_init__(test_mode=True)
    ex._job_submission_executor.shutdown(wait=False)
    return ex


@pytest.mark.parametrize("prefix", ["", "leukoquant"])
def test_two_runs_never_share_file_names(monkeypatch, tmp_path, prefix):
    a = _new_executor(monkeypatch, tmp_path, prefix)
    b = _new_executor(monkeypatch, tmp_path, prefix)
    assert a.run_tag != b.run_tag
    for chunk in (1, 2, 7):
        assert a._chunk_stem(chunk) != b._chunk_stem(chunk)
        assert a._chunk_stem(chunk).startswith(a.run_tag)


def test_tag_is_the_random_part_even_with_a_prefix(monkeypatch, tmp_path):
    ex = _new_executor(monkeypatch, tmp_path, prefix="leukoquant")
    assert not "leukoquant".startswith(ex.run_tag)
    assert ex.run_uuid.split("_", 1)[1].startswith(ex.run_tag)


def test_tag_matches_job_name_suffix_without_prefix(monkeypatch, tmp_path):
    ex = _new_executor(monkeypatch, tmp_path)
    # submit_string.py names jobs "<rule>_<run_uuid[:8]>".
    assert ex.run_uuid[:8] == ex.run_tag
