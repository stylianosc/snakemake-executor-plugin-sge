"""Regression test for clean_old_logs() deleting still-in-use empty
directories that belong to a queued (not-yet-started) SGE array job
submitted by a DIFFERENT Snakemake invocation sharing the same
.snakemake/sge_logs tree.

Bug: shutdown() runs clean_old_logs() at the end of EVERY invocation and
removes any *empty* directory under sge_logdir_default with no age check.
A per-sub-range log directory is mkdir'd at submission time but stays
empty until its first task actually starts -- if the job is still queued
when an unrelated invocation finishes and cleans up, the directory gets
deleted before the queued job ever uses it, and every task in that array
then fails immediately with "can't open output file: No such file or
directory" as soon as SGE dispatches it.
"""
import os
import time
from pathlib import Path
from types import SimpleNamespace

from snakemake_executor_plugin_sge import Executor


def _make_executor_stub(logdir: Path, age_cutoff_days: float):
    return SimpleNamespace(
        sge_logdir_default=logdir,
        workflow=SimpleNamespace(
            executor_settings=SimpleNamespace(
                delete_logfiles_older_than=age_cutoff_days,
                keep_successful_logs=False,
            )
        ),
        logger=SimpleNamespace(debug=lambda *a, **k: None, warning=lambda *a, **k: None),
    )


def test_fresh_empty_directory_survives_cleanup(tmp_path):
    """A just-mkdir'd, still-empty log directory (e.g. for a queued-but-not-
    yet-started SGE task from a different invocation) must not be deleted."""
    logdir = tmp_path / "sge_logs"
    pending_dir = logdir / "chunk1_1_1"
    pending_dir.mkdir(parents=True)

    stub = _make_executor_stub(logdir, age_cutoff_days=1)
    Executor.clean_old_logs(stub)

    assert pending_dir.exists(), "still-pending job's empty log dir was wrongly deleted"


def test_genuinely_stale_empty_directory_is_removed(tmp_path):
    """An empty directory that's actually old (older than the cutoff, e.g.
    left over from a run that finished long ago) should still be reclaimed."""
    logdir = tmp_path / "sge_logs"
    stale_dir = logdir / "chunk1_1_1"
    stale_dir.mkdir(parents=True)

    old_time = time.time() - (2 * 86400)  # 2 days ago
    os.utime(stale_dir, (old_time, old_time))

    stub = _make_executor_stub(logdir, age_cutoff_days=1)
    Executor.clean_old_logs(stub)

    assert not stale_dir.exists(), "genuinely stale empty log dir should be reclaimed"


def test_old_log_files_still_deleted(tmp_path):
    """Regression check: the original per-file age-based deletion still works."""
    logdir = tmp_path / "sge_logs" / "chunk1_1_1"
    logdir.mkdir(parents=True)
    old_file = logdir / "123.1.log"
    old_file.write_text("done")
    old_time = time.time() - (2 * 86400)
    os.utime(old_file, (old_time, old_time))

    stub = _make_executor_stub(logdir.parent, age_cutoff_days=1)
    Executor.clean_old_logs(stub)

    assert not old_file.exists()


def test_recent_log_files_kept(tmp_path):
    logdir = tmp_path / "sge_logs" / "chunk1_1_1"
    logdir.mkdir(parents=True)
    recent_file = logdir / "123.1.log"
    recent_file.write_text("done")

    stub = _make_executor_stub(logdir.parent, age_cutoff_days=1)
    Executor.clean_old_logs(stub)

    assert recent_file.exists()
