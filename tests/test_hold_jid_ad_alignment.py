"""End-to-end simulation of a fan-in dependency pattern with -hold_jid_ad.

This exercises the real array-hold resolution path of the executor
(``_split_contiguous_ranges`` → ``_split_by_upstream_boundaries`` →
``_resolve_array_holds``) against the situation that broke on the UCL cluster:

  * a fan-in where one rule (``sink``) depends on several upstream rules that
    run *simultaneously* (``branch_a``, ``branch_b``, ``branch_c``);
  * a *sequential* chain feeding those (``root`` → each branch);
  * subjects that start "from a different level" — some already have certain
    steps completed, so each rule is submitted over a *different* set of task
    indices, producing sparse, non-aligned array ranges.

The rule names are deliberately generic; the shape is what matters.

The invariant asserted here is the one UCL SGE enforces (verified empirically):
every upstream array job named in a downstream ``-hold_jid_ad`` list must have
the *exact same* ``-t`` range as that downstream array sub-range.  A single
upstream with a wider/narrower range, or a comma-separated list containing any
range-mismatched job, is rejected by qsub with:

    "This array job must have the same range of sub-tasks as the dependent
     array job specified with -hold_jid_ad"

Comma-separated lists themselves are fine — only range equality matters.  The
test also checks that *no* upstream dependency is ever silently dropped: every
upstream of every task is covered by either ``-hold_jid_ad`` (per-task, same
range) or whole-job ``-hold_jid``.
"""

import logging
import types

from snakemake_executor_plugin_sge import Executor


# --------------------------------------------------------------------------- #
# Minimal job / DAG model
# --------------------------------------------------------------------------- #
class Job:
    """A stand-in for a Snakemake job: identified by (rule, subject)."""

    def __init__(self, rule, subject):
        self.rule = rule
        self.subject = subject
        self.name = f"{rule}:{subject}"

    def __repr__(self):
        return self.name


class Simulator:
    """Drives the executor's real hold-resolution methods over a fake DAG.

    ``needed[rule]`` is the set of subjects for which that rule must actually
    run (subjects missing from the set already have that step's output, so no
    job is created for them — mirroring Snakemake skipping satisfied targets).
    """

    def __init__(self, subjects, rule_deps, needed):
        self.subjects = subjects
        self.rule_deps = rule_deps          # rule -> list of upstream rules
        self.needed = needed                # rule -> set(subjects)
        # Stable global subject index (1-based), shared across all rules.
        self.idx = {s: i + 1 for i, s in enumerate(subjects)}

        # One Job object per (rule, subject) that is actually submitted.
        self.jobs = {
            (r, s): Job(r, s)
            for r in rule_deps
            for s in needed[r]
        }

        # Build DAG dependencies: a job depends on its upstream-rule jobs for
        # the same subject, but only where that upstream job exists (i.e. the
        # upstream step wasn't already completed for that subject).
        deps = {}
        for (r, s), job in self.jobs.items():
            up = {}
            for ur in rule_deps[r]:
                if (ur, s) in self.jobs:
                    up[self.jobs[(ur, s)]] = None
            deps[job] = up

        # A bare Executor with only the state the hold methods touch.
        ex = Executor.__new__(Executor)
        ex.logger = logging.getLogger("sim")
        ex.logger.addHandler(logging.NullHandler())
        ex.workflow = types.SimpleNamespace(
            dag=types.SimpleNamespace(dependencies=deps)
        )
        ex._job_to_sge = {}
        ex._array_job_range = {}
        self.ex = ex
        self._next_jobid = 1000

    def _new_jobid(self):
        self._next_jobid += 1
        return f"J{self._next_jobid}"

    def submit_rule(self, rule):
        """Simulate submitting one rule as SGE array sub-jobs.

        Returns a list of ``(sub_start, sub_end, hold_ad, hold_jid_list)`` for
        each sub-range actually submitted, so the caller can assert on them.
        """
        run_subjects = sorted(self.needed[rule], key=lambda s: self.idx[s])
        idxs = [self.idx[s] for s in run_subjects]
        idx_to_job = {self.idx[s]: self.jobs[(rule, s)] for s in run_subjects}

        contiguous = self.ex._split_contiguous_ranges(idxs)
        sub_ranges = self.ex._split_by_upstream_boundaries(contiguous, idx_to_job)

        submitted = []
        for sub_start, sub_end, sub_idxs in sub_ranges:
            sub_jobs = [idx_to_job[i] for i in sub_idxs]
            hold_ad, hold_jid = self.ex._resolve_array_holds(
                sub_jobs, sub_start, sub_end
            )

            jobid = self._new_jobid()
            for i, job in zip(sub_idxs, sub_jobs):
                self.ex._job_to_sge[job] = (jobid, i)
            self.ex._array_job_range[jobid] = (sub_start, sub_end)

            submitted.append((sub_start, sub_end, hold_ad, hold_jid))
        return submitted


# --------------------------------------------------------------------------- #
# Invariant checks (mirror the SGE behaviour proved on the cluster)
# --------------------------------------------------------------------------- #
def assert_sge_would_accept(sim, sub_start, sub_end, hold_ad, hold_jid):
    """Every -hold_jid_ad job must share this sub-range's exact -t range."""
    down_range = (sub_start, sub_end)
    if hold_ad:
        for jid in hold_ad.split(","):
            assert sim.ex._array_job_range[jid] == down_range, (
                f"SGE would REJECT: -hold_jid_ad {jid} has range "
                f"{sim.ex._array_job_range[jid]} != downstream {down_range}"
            )


def assert_no_dependency_dropped(sim, rule, sub_start, sub_end, hold_ad, hold_jid):
    """Every upstream of every task in the sub-range must be held somehow."""
    ad_set = set(hold_ad.split(",")) if hold_ad else set()
    jid_set = set(hold_jid)
    held = ad_set | jid_set
    for s in sim.subjects:
        i = sim.idx[s]
        if not (sub_start <= i <= sub_end):
            continue
        job = sim.jobs.get((rule, s))
        if job is None:
            continue
        for up_job, up_jobid, _tidx in sim.ex._upstream_ext_ids(job):
            assert up_jobid in held, (
                f"Dropped dependency: {rule}:{s} needs {up_job} "
                f"(job {up_jobid}) but it is in neither hold list"
            )


# --------------------------------------------------------------------------- #
# The fan-in scenario:
#
#        root  (sequential parent, one array over all subjects)
#       /  |  \
#  branch_a branch_b branch_c   (run simultaneously)
#       \  |  /
#         sink  (fan-in: depends on all three branches)
# --------------------------------------------------------------------------- #
RULE_DEPS = {
    "root": [],
    "branch_a": ["root"],
    "branch_b": ["root"],
    "branch_c": ["root"],
    "sink": ["branch_a", "branch_b", "branch_c"],
}
SUBMIT_ORDER = ["root", "branch_a", "branch_b", "branch_c", "sink"]
BRANCHES = ["branch_a", "branch_b", "branch_c"]


def _run_scenario(subjects, needed):
    sim = Simulator(subjects, RULE_DEPS, needed)
    all_submissions = {}
    for rule in SUBMIT_ORDER:
        subs = sim.submit_rule(rule)
        all_submissions[rule] = subs
        for sub_start, sub_end, hold_ad, hold_jid in subs:
            assert_sge_would_accept(sim, sub_start, sub_end, hold_ad, hold_jid)
            assert_no_dependency_dropped(
                sim, rule, sub_start, sub_end, hold_ad, hold_jid
            )
    return sim, all_submissions


def test_fresh_run_all_subjects_use_per_task_hold():
    """Fresh run: every rule spans -t 1-N, so sink holds on all three branch
    arrays per-task via one comma-separated -hold_jid_ad."""
    subjects = [f"sub{i:02d}" for i in range(1, 9)]
    needed = {r: set(subjects) for r in RULE_DEPS}
    sim, subs = _run_scenario(subjects, needed)

    # sink is one contiguous 1-8 array holding on all three branches per-task.
    assert len(subs["sink"]) == 1
    sub_start, sub_end, hold_ad, hold_jid = subs["sink"][0]
    assert (sub_start, sub_end) == (1, 8)
    assert hold_ad is not None
    assert len(hold_ad.split(",")) == 3, hold_ad
    assert hold_jid == []


def test_staggered_completion_stays_sge_valid():
    """Subjects start from different levels: each branch runs over a different,
    sparse subject set.  Holds must remain SGE-valid and complete."""
    subjects = [f"sub{i:02d}" for i in range(1, 9)]
    alls = set(subjects)
    needed = {
        "root": set(alls),                          # 1-8
        # branch_a already done for sub02  -> runs 1, 3-8 (sparse)
        "branch_a": alls - {"sub02"},
        # branch_b already done for sub05  -> runs 1-4, 6-8 (sparse)
        "branch_b": alls - {"sub05"},
        "branch_c": set(alls),                      # 1-8
        "sink": set(alls),                          # 1-8, mixed upstreams
    }
    sim, subs = _run_scenario(subjects, needed)

    # Sanity: the sparse branches really did split into multiple sub-ranges.
    assert len(subs["branch_a"]) >= 2
    assert len(subs["branch_b"]) >= 2

    # sink must have been split so each piece is SGE-valid; at least one sink
    # sub-range should still achieve a per-task -hold_jid_ad (the region where
    # all three branch ranges coincide).
    got_per_task = any(hold_ad for _, _, hold_ad, _ in subs["sink"])
    assert got_per_task, subs["sink"]


def test_sequential_partial_falls_back_cleanly():
    """A sequential child whose parent spans a wider range must fall back to
    whole-job -hold_jid (SGE would reject a subset -hold_jid_ad)."""
    subjects = [f"sub{i:02d}" for i in range(1, 6)]
    alls = set(subjects)
    needed = {
        "root": set(alls),                    # 1-5 (single wide array)
        "branch_a": {"sub03", "sub04"},       # only 3-4 need branch_a
        "branch_b": set(),
        "branch_c": set(),
        "sink": set(),
    }
    sim = Simulator(subjects, RULE_DEPS, needed)
    sim.submit_rule("root")
    branch = sim.submit_rule("branch_a")

    # branch_a runs 3-4 while root is 1-5: ranges differ, so root must be held
    # whole-job, NOT per-task (which SGE would reject).
    assert len(branch) == 1
    sub_start, sub_end, hold_ad, hold_jid = branch[0]
    assert (sub_start, sub_end) == (3, 4)
    assert hold_ad is None, hold_ad
    assert len(hold_jid) == 1
    assert_sge_would_accept(sim, sub_start, sub_end, hold_ad, hold_jid)
    assert_no_dependency_dropped(
        sim, "branch_a", sub_start, sub_end, hold_ad, hold_jid
    )


def test_exhaustive_random_completion_levels():
    """Fuzz many random completion states; every emitted hold set must be
    SGE-valid and drop no dependency."""
    import random

    rng = random.Random(20260708)
    subjects = [f"sub{i:02d}" for i in range(1, 11)]
    alls = set(subjects)

    for _ in range(200):
        needed = {"root": set(alls)}
        for r in BRANCHES:
            # Each subject independently may already have this step done.
            needed[r] = {s for s in subjects if rng.random() > 0.35}
        # sink runs for a random non-empty subset.
        needed["sink"] = {s for s in subjects if rng.random() > 0.2}

        sim = Simulator(subjects, RULE_DEPS, needed)
        for rule in SUBMIT_ORDER:
            for sub_start, sub_end, hold_ad, hold_jid in sim.submit_rule(rule):
                assert_sge_would_accept(sim, sub_start, sub_end, hold_ad, hold_jid)
                assert_no_dependency_dropped(
                    sim, rule, sub_start, sub_end, hold_ad, hold_jid
                )
