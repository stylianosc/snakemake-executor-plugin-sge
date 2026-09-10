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
        # Real Snakemake jobs expose .rule as a Rule object with .name, not
        # a bare string -- match that shape since the plugin reads
        # job.rule.name (_split_by_downstream_boundaries).
        self.rule = types.SimpleNamespace(name=rule)
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

        # Forward edges (dag.depending): the exact inverse of deps, mirroring
        # what the real DAG.update() populates alongside dependencies before
        # any submission begins.
        depending = {job: {} for job in self.jobs.values()}
        for job, ups in deps.items():
            for up_job in ups:
                depending[up_job][job] = None

        # A bare Executor with only the state the hold methods touch.
        ex = Executor.__new__(Executor)
        ex.logger = logging.getLogger("sim")
        ex.logger.addHandler(logging.NullHandler())
        ex.workflow = types.SimpleNamespace(
            dag=types.SimpleNamespace(dependencies=deps, depending=depending)
        )
        ex._job_to_sge = {}
        ex._array_job_range = {}
        self.ex = ex
        self._next_jobid = 1000

    def _new_jobid(self):
        self._next_jobid += 1
        return f"J{self._next_jobid}"

    def submit_rule(self, rule, use_downstream_split=True):
        """Simulate submitting one rule as SGE array sub-jobs.

        ``use_downstream_split=False`` reproduces the plugin's behavior
        *before* _split_by_downstream_boundaries existed -- both code paths
        are real methods on the real Executor (nothing is stubbed out or
        skipped via a nonexistent function), this just omits one real step
        from the pipeline to let "before" and "after" be compared directly
        against the SAME scenario without git-stashing the source.

        Returns a list of ``(sub_start, sub_end, hold_ad, hold_jid_list)`` for
        each sub-range actually submitted, so the caller can assert on them.
        """
        run_subjects = sorted(self.needed[rule], key=lambda s: self.idx[s])
        idxs = [self.idx[s] for s in run_subjects]
        idx_to_job = {self.idx[s]: self.jobs[(rule, s)] for s in run_subjects}

        contiguous = self.ex._split_contiguous_ranges(idxs)
        if use_downstream_split:
            contiguous = self.ex._split_by_downstream_boundaries(contiguous, idx_to_job)
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


def test_sequential_partial_gets_per_task_hold_via_downstream_split():
    """Regression test for the real cluster case (2026-09-01): root is
    submitted as one wide array (1-5), but branch_a's own need-set is only
    a fragment of it (3-4 -- e.g. subjects 1,2,5 had a data gap upstream
    of root, or simply never needed branch_a). Before
    _split_by_downstream_boundaries existed, root had no way to know
    branch_a only needed 3-4, so it stayed one monolithic 1-5 array and
    branch_a's 3-4 fragment could never find an exact-range match -- this
    exact assertion (hold_ad is None, whole-job fallback) used to be the
    documented-correct behavior in this test (see git history) before the
    fix. With the fix, root pre-splits itself at the point where branch_a's
    need-presence changes (a boundary at 3-4 vs. 1-2/5), so branch_a's
    fragment finds a matching root sub-range and gets a real -hold_jid_ad.

    This test FAILS against the pre-fix code (hold_ad was None there) and
    PASSES against the fix -- i.e. it is the direct regression test for
    _split_by_downstream_boundaries, not just a generic invariant check.
    """
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

    # branch_a runs 3-4; with downstream-boundary splitting, root itself was
    # pre-split so a 3-4 root sub-range exists, letting branch_a get a real
    # per-task -hold_jid_ad instead of falling back to whole-job -hold_jid.
    assert len(branch) == 1
    sub_start, sub_end, hold_ad, hold_jid = branch[0]
    assert (sub_start, sub_end) == (3, 4)
    assert hold_ad is not None, (
        "expected a per-task -hold_jid_ad now that root pre-splits at "
        "branch_a's own need-set boundary -- got whole-job fallback instead, "
        "meaning _split_by_downstream_boundaries did not fire as expected"
    )
    assert hold_jid == []
    assert_sge_would_accept(sim, sub_start, sub_end, hold_ad, hold_jid)
    assert_no_dependency_dropped(
        sim, "branch_a", sub_start, sub_end, hold_ad, hold_jid
    )


def test_nonarray_upstream_still_falls_back_cleanly():
    """A downstream job whose upstream was submitted as a single non-array
    task (task_idx is None) has no per-task index to align against at all
    -- this must still cleanly fall back to whole-job -hold_jid regardless
    of the downstream-boundary-splitting fix, since there is no array range
    on the upstream side to split in the first place. Matches the OTHER
    real cluster case this session (a single-subject z-score job with no
    matching array range on either side)."""
    subjects = [f"sub{i:02d}" for i in range(1, 6)]
    sim = Simulator(subjects, RULE_DEPS, {
        "root": set(subjects),
        "branch_a": {"sub03"},
        "branch_b": set(),
        "branch_c": set(),
        "sink": set(),
    })
    sim.submit_rule("root")

    # Manually downgrade root's recorded submission to a non-array (single
    # task) entry for sub03, as if it had been submitted individually
    # rather than as part of root's 1-5 array.
    root_job = sim.jobs[("root", "sub03")]
    single_jobid = sim._new_jobid()
    sim.ex._job_to_sge[root_job] = (single_jobid, None)
    sim.ex._array_job_range.pop(single_jobid, None)

    branch = sim.submit_rule("branch_a")
    assert len(branch) == 1
    sub_start, sub_end, hold_ad, hold_jid = branch[0]
    assert hold_ad is None, hold_ad
    assert hold_jid == [single_jobid]
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


# --------------------------------------------------------------------------- #
# Real cluster case (2026-09-01, EPAD): tracula_gif -> tractqc_gif ->
# metrics_gif. tractqc_gif was submitted as one whole array over global
# subject indices 164-250 (real job 7314183); metrics_gif's own need-set
# within that range was fragmented into exactly three pieces -- {236},
# {238,239,240}, {244..250} -- matching the real observed jobs 7314218
# (1 task), 7314219 (3 tasks), 7314220 (7 tasks). Real subject IDs aren't
# reproduced (not needed for this), but the rule names, the range, and the
# exact fragmentation shape are the real ones, not placeholders.
# --------------------------------------------------------------------------- #
EPAD_RULE_DEPS = {
    "tracula_gif": [],
    "tractqc_gif": ["tracula_gif"],
    "metrics_gif": ["tractqc_gif"],
}
EPAD_SUBJECTS = [f"sub{i}" for i in range(164, 251)]  # 87 subjects, real range
METRICS_GIF_NEEDED = {"sub236", "sub238", "sub239", "sub240",
                       "sub244", "sub245", "sub246", "sub247", "sub248", "sub249", "sub250"}


def _tractqc_jobid_for(sim, subject):
    return sim.ex._job_to_sge[sim.jobs[("tractqc_gif", subject)]][0]


def _real_range(sim, lo, hi):
    """Real subject numbers (e.g. 236, 250) -> the Simulator's own internal
    1-based global index range -- the Simulator assigns indices by list
    position, not by parsing the subject name, so real subject numbers and
    internal indices are different numbering schemes related by a fixed
    offset. Expressing expected ranges this way keeps the test readable in
    real subject numbers while staying correct against however the
    Simulator actually assigns indices."""
    return (sim.idx[f"sub{lo}"], sim.idx[f"sub{hi}"])


def test_real_epad_metrics_gif_case_reproduces_old_bug():
    """Without _split_by_downstream_boundaries: tractqc_gif stays one whole
    164-250 array, so every metrics_gif fragment falls back to whole-job
    -hold_jid on it -- reproducing job 7314218/7314219/7314220's real
    situation exactly (blocked on the entire tractqc_gif array, including
    subjects they have nothing to do with)."""
    needed = {
        "tracula_gif": set(EPAD_SUBJECTS),
        "tractqc_gif": set(EPAD_SUBJECTS),
        "metrics_gif": set(METRICS_GIF_NEEDED),
    }
    sim = Simulator(EPAD_SUBJECTS, EPAD_RULE_DEPS, needed)
    sim.submit_rule("tracula_gif", use_downstream_split=False)
    tractqc_subs = sim.submit_rule("tractqc_gif", use_downstream_split=False)

    # tractqc_gif: one whole array, exactly like the real job 7314183.
    assert [(s, e) for s, e, _, _ in tractqc_subs] == [_real_range(sim, 164, 250)]
    tractqc_jobid = _tractqc_jobid_for(sim, "sub164")

    metrics_subs = sim.submit_rule("metrics_gif", use_downstream_split=False)

    # metrics_gif still fragments into 3 pieces on its OWN need-set alone
    # (that part doesn't need the fix) -- matches the real 236 / 238-240 /
    # 244-250 job split.
    assert [(s, e) for s, e, _, _ in metrics_subs] == [
        _real_range(sim, 236, 236), _real_range(sim, 238, 240), _real_range(sim, 244, 250),
    ]

    # The bug: every fragment falls back to the SAME whole-job tractqc_gif
    # hold, with no per-task -hold_jid_ad at all.
    for sub_start, sub_end, hold_ad, hold_jid in metrics_subs:
        assert hold_ad is None, (sub_start, sub_end, hold_ad)
        assert hold_jid == [tractqc_jobid], (sub_start, sub_end, hold_jid, tractqc_jobid)
        assert_sge_would_accept(sim, sub_start, sub_end, hold_ad, hold_jid)
        assert_no_dependency_dropped(sim, "metrics_gif", sub_start, sub_end, hold_ad, hold_jid)


def test_real_epad_metrics_gif_case_fixed():
    """With the fix: tractqc_gif pre-splits itself at metrics_gif's own
    need-set boundaries, so each metrics_gif fragment finds an
    exact-range tractqc_gif counterpart and gets a real per-task
    -hold_jid_ad instead of the whole-job fallback."""
    needed = {
        "tracula_gif": set(EPAD_SUBJECTS),
        "tractqc_gif": set(EPAD_SUBJECTS),
        "metrics_gif": set(METRICS_GIF_NEEDED),
    }
    sim = Simulator(EPAD_SUBJECTS, EPAD_RULE_DEPS, needed)
    sim.submit_rule("tracula_gif")
    tractqc_subs = sim.submit_rule("tractqc_gif")

    # tractqc_gif is now pre-split at metrics_gif's need-set boundaries:
    # 164-235 nobody-downstream-needs-differently, 236 needed, 237 not
    # needed, 238-240 needed, 241-243 not needed, 244-250 needed.
    expected_tractqc = [(164, 235), (236, 236), (237, 237), (238, 240), (241, 243), (244, 250)]
    assert [(s, e) for s, e, _, _ in tractqc_subs] == [_real_range(sim, lo, hi) for lo, hi in expected_tractqc]
    tractqc_range_to_jobid = {
        (s, e): _tractqc_jobid_for(sim, EPAD_SUBJECTS[s - 1]) for s, e, _, _ in tractqc_subs
    }

    metrics_subs = sim.submit_rule("metrics_gif")

    # Same 3 real fragments as before -- the fix doesn't change WHAT
    # metrics_gif submits, only what it can hold on.
    assert [(s, e) for s, e, _, _ in metrics_subs] == [
        _real_range(sim, 236, 236), _real_range(sim, 238, 240), _real_range(sim, 244, 250),
    ]

    for sub_start, sub_end, hold_ad, hold_jid in metrics_subs:
        expected_jobid = tractqc_range_to_jobid[(sub_start, sub_end)]
        assert hold_ad == expected_jobid, (
            f"metrics_gif {sub_start}-{sub_end}: expected per-task hold on "
            f"{expected_jobid}, got hold_ad={hold_ad!r} hold_jid={hold_jid!r}"
        )
        assert hold_jid == []
        assert_sge_would_accept(sim, sub_start, sub_end, hold_ad, hold_jid)
        assert_no_dependency_dropped(sim, "metrics_gif", sub_start, sub_end, hold_ad, hold_jid)


def test_real_epad_case_two_downstream_consumers_different_gaps():
    """Two rules consume tractqc_gif with DIFFERENT, only partially
    overlapping need-sets (metrics_gif's real gap pattern, plus a second
    consumer needing an unrelated pair of subjects). The upstream split
    must take the union of both rules' boundary points -- neither consumer
    should regress to a whole-job fallback because of the other's
    different gap shape."""
    qc_report_needed = {"sub170", "sub236"}  # deliberately unrelated to metrics_gif's gaps
    rule_deps = dict(EPAD_RULE_DEPS, qc_report_gif=["tractqc_gif"])
    needed = {
        "tracula_gif": set(EPAD_SUBJECTS),
        "tractqc_gif": set(EPAD_SUBJECTS),
        "metrics_gif": set(METRICS_GIF_NEEDED),
        "qc_report_gif": set(qc_report_needed),
    }
    sim = Simulator(EPAD_SUBJECTS, rule_deps, needed)
    sim.submit_rule("tracula_gif")
    tractqc_subs = sim.submit_rule("tractqc_gif")

    expected_tractqc = [
        (164, 169), (170, 170), (171, 235), (236, 236),
        (237, 237), (238, 240), (241, 243), (244, 250),
    ]
    assert [(s, e) for s, e, _, _ in tractqc_subs] == [_real_range(sim, lo, hi) for lo, hi in expected_tractqc]
    tractqc_range_to_jobid = {
        (s, e): _tractqc_jobid_for(sim, EPAD_SUBJECTS[s - 1]) for s, e, _, _ in tractqc_subs
    }

    metrics_subs = sim.submit_rule("metrics_gif")
    qc_subs = sim.submit_rule("qc_report_gif")

    assert [(s, e) for s, e, _, _ in metrics_subs] == [
        _real_range(sim, 236, 236), _real_range(sim, 238, 240), _real_range(sim, 244, 250),
    ]
    assert [(s, e) for s, e, _, _ in qc_subs] == [_real_range(sim, 170, 170), _real_range(sim, 236, 236)]

    # Both consumers get a real per-task hold on tractqc_gif -- the union
    # split serves each rule's own shape, not just whichever was submitted
    # first, and 236 is shared by both without conflict.
    for rule, subs in (("metrics_gif", metrics_subs), ("qc_report_gif", qc_subs)):
        for sub_start, sub_end, hold_ad, hold_jid in subs:
            expected_jobid = tractqc_range_to_jobid[(sub_start, sub_end)]
            assert hold_ad == expected_jobid, (
                f"{rule} {sub_start}-{sub_end}: expected hold on {expected_jobid}, "
                f"got hold_ad={hold_ad!r} hold_jid={hold_jid!r}"
            )
            assert hold_jid == []
            assert_sge_would_accept(sim, sub_start, sub_end, hold_ad, hold_jid)
            assert_no_dependency_dropped(sim, rule, sub_start, sub_end, hold_ad, hold_jid)
