"""
SGE job cancellation via qdel.

Called when Snakemake is interrupted (Ctrl-C / SIGTERM) to clean up all
still-running jobs.  For array tasks we cancel the *parent* job ID, which
implicitly kills every task of that array.
"""

import subprocess
from typing import List

from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo


def cancel_sge_jobs(active_jobs: List[SubmittedJobInfo], logger) -> None:
    """Issue a single ``qdel`` call for all unique parent job IDs."""
    if not active_jobs:
        return

    # Deduplicate: '12345.3' and '12345.7' both cancel by 'qdel 12345'
    parent_ids = sorted({j.external_jobid.split(".")[0] for j in active_jobs})
    id_str = " ".join(parent_ids)
    logger.info(f"Cancelling SGE jobs: {id_str}")
    try:
        subprocess.run(
            f"qdel {id_str}",
            shell=True, check=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as exc:
        logger.warning(
            f"qdel returned non-zero when cancelling {id_str}: "
            f"{exc.stderr.decode(errors='replace').strip()}"
        )
