"""Auto-clearing of Eqw jobs whose only error is a not-yet-visible log directory."""
from unittest import mock

from snakemake_executor_plugin_sge import job_status_query as jsq

CHDIR = "error reason  1: 10/03/2026 [1:2]: error: can't chdir to /x/y: No such file or directory\n"
OTHER = "error reason  1: 10/03/2026 [1:2]: error: shell exited with 1\n"


def _run(qstat_out):
    """Return a fake check_output answering qstat -j with qstat_out and qmod with ok."""
    def fake(cmd, **kw):
        return qstat_out if cmd.startswith("qstat") else "cleared"
    return fake


def setup_function():
    jsq._AUTO_CLEARS.clear()


def test_chdir_error_is_cleared_repeatedly_up_to_the_cap():
    with mock.patch.object(jsq.subprocess, "check_output", side_effect=_run(CHDIR)):
        results = [jsq._try_clear_error_state("42", mock.Mock()) for _ in range(7)]
    assert results == [True] * jsq._MAX_AUTO_CLEARS + [False] * (7 - jsq._MAX_AUTO_CLEARS)


def test_other_errors_are_never_cleared():
    with mock.patch.object(jsq.subprocess, "check_output", side_effect=_run(OTHER)):
        assert jsq._try_clear_error_state("43", mock.Mock()) is False


def test_mixed_reasons_are_not_cleared():
    with mock.patch.object(jsq.subprocess, "check_output", side_effect=_run(CHDIR + OTHER)):
        assert jsq._try_clear_error_state("44", mock.Mock()) is False
