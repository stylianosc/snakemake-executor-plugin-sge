"""SGE job cancellation via qdel.

Called when Snakemake is interrupted (e.g. Ctrl+C) to clean up all
submitted jobs that have not yet finished.
"""

import subprocess
from typing import List

from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo


def cancel_sge_jobs(
    active_jobs: List[SubmittedJobInfo],
    logger,
) -> None:
    """Cancel all SGE jobs in *active_jobs* using a single qdel call.

    For array-job tasks (external_jobid of the form ``12345.3``) we cancel
    the parent array job, which implicitly cancels all tasks.
    """
    if not active_jobs:
        return

    # Collect unique parent IDs to avoid issuing the same qdel twice
    parent_ids: set = set()
    for j in active_jobs:
        base = j.external_jobid.split(".")[0]
        parent_ids.add(base)

    ids_str = " ".join(sorted(parent_ids))
    logger.info(f"Cancelling SGE jobs: {ids_str}")
    try:
        subprocess.run(
            f"qdel {ids_str}",
            shell=True,
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as exc:
        logger.warning(
            f"qdel returned non-zero exit code when cancelling jobs: "
            f"{exc.stderr.decode().strip()}"
        )
