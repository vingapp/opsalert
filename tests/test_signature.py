"""Signature tests — pinned to REAL production alert messages.

Every fixture in this file is a verbatim message captured from the production
``opsalert`` table on 2026-08-30 (scratchpad/prod-message-fixtures.jsonl). The
normalizer's whole job is to be right about the traffic that actually exists,
so it is graded against that traffic rather than against invented examples.

The rule being pinned is asymmetric on purpose. Over-merging is the dangerous
error — two different failures in one condition means acknowledging one hides
the other forever — so the normalizer only collapses what is unambiguously
per-occurrence noise, and emit sites that need exact identity pass ``params``.
"""
# The fixtures below are verbatim production messages; wrapping them would
# stop them being verbatim, which is the only thing that makes them evidence.
# ruff: noqa: E501
from opsalert.signature import condition_signature, normalize_message

MANAGER_FAILURE = [
    "trace writer flush failed: (pymysql.err.OperationalError) (1213, 'Deadlock found when trying to get lock; try restarting transaction')\n[SQL: UPDATE trace_step SET status=%(status)s, completed_at=%(completed_at)s WHERE trace_step.step_id = %(step_id_1)s]\n[parameters: {'status': 'completed', 'completed_at': datetime.datetime(2026, 8, 30, 20, 49, 56, 843067), 'step_id_1': '08f16cbfcbbb456a'}]\n(Background on this error at: https://sqlalche.me/e/20/e3q8)",
    "trace writer flush failed: (pymysql.err.OperationalError) (1213, 'Deadlock found when trying to get lock; try restarting transaction')\n[SQL: UPDATE trace_step SET status=%(status)s, completed_at=%(completed_at)s WHERE trace_step.step_id = %(step_id_1)s]\n[parameters: {'status': 'completed', 'completed_at': datetime.datetime(2026, 8, 30, 20, 34, 44, 733499), 'step_id_1': 'c9e3ad4a98594489'}]\n(Background on this error at: https://sqlalche.me/e/20/e3q8)",
    "trace writer flush failed: (pymysql.err.OperationalError) (1213, 'Deadlock found when trying to get lock; try restarting transaction')\n[SQL: UPDATE trace_step SET status=%(status)s, completed_at=%(completed_at)s WHERE trace_step.step_id = %(step_id_1)s]\n[parameters: {'status': 'completed', 'completed_at': datetime.datetime(2026, 8, 30, 19, 38, 56, 492277), 'step_id_1': '3415bd63af5b4fe9'}]\n(Background on this error at: https://sqlalche.me/e/20/e3q8)",
    "trace writer flush failed: (pymysql.err.OperationalError) (1213, 'Deadlock found when trying to get lock; try restarting transaction')\n[SQL: UPDATE trace_step SET status=%(status)s, completed_at=%(completed_at)s WHERE trace_step.step_id = %(step_id_1)s]\n[parameters: {'status': 'completed', 'completed_at': datetime.datetime(2026, 8, 30, 19, 38, 46, 166556), 'step_id_1': 'ec13a4698ace4117'}]\n(Background on this error at: https://sqlalche.me/e/20/e3q8)",
    "trace writer flush failed: (pymysql.err.OperationalError) (1213, 'Deadlock found when trying to get lock; try restarting transaction')\n[SQL: UPDATE trace_step SET status=%(status)s, completed_at=%(completed_at)s WHERE trace_step.step_id = %(step_id_1)s]\n[parameters: {'status': 'completed', 'completed_at': datetime.datetime(2026, 8, 30, 19, 38, 6, 221415), 'step_id_1': '495127f4901f4b05'}]\n(Background on this error at: https://sqlalche.me/e/20/e3q8)",
    "trace writer flush failed: (pymysql.err.OperationalError) (1213, 'Deadlock found when trying to get lock; try restarting transaction')\n[SQL: UPDATE trace_step SET status=%(status)s, completed_at=%(completed_at)s WHERE trace_step.step_id = %(step_id_1)s]\n[parameters: {'status': 'completed', 'completed_at': datetime.datetime(2026, 8, 30, 19, 37, 56, 399778), 'step_id_1': 'c9b33de1d7434284'}]\n(Background on this error at: https://sqlalche.me/e/20/e3q8)",
    "trace writer flush failed: (pymysql.err.OperationalError) (1213, 'Deadlock found when trying to get lock; try restarting transaction')\n[SQL: UPDATE trace_step SET status=%(status)s, completed_at=%(completed_at)s WHERE trace_step.step_id = %(step_id_1)s]\n[parameters: {'status': 'completed', 'completed_at': datetime.datetime(2026, 8, 30, 19, 36, 19, 266073), 'step_id_1': '86d58c0c2c5e4930'}]\n(Background on this error at: https://sqlalche.me/e/20/e3q8)",
    "trace writer flush failed: (pymysql.err.OperationalError) (1213, 'Deadlock found when trying to get lock; try restarting transaction')\n[SQL: UPDATE trace_step SET status=%(status)s, completed_at=%(completed_at)s WHERE trace_step.step_id = %(step_id_1)s]\n[parameters: {'status': 'completed', 'completed_at': datetime.datetime(2026, 8, 30, 19, 34, 4, 180649), 'step_id_1': 'cc74452bb32441d7'}]\n(Background on this error at: https://sqlalche.me/e/20/e3q8)",
    "trace writer flush failed: (pymysql.err.OperationalError) (1213, 'Deadlock found when trying to get lock; try restarting transaction')\n[SQL: UPDATE trace_step SET status=%(status)s, completed_at=%(completed_at)s WHERE trace_step.step_id = %(step_id_1)s]\n[parameters: {'status': 'completed', 'completed_at': datetime.datetime(2026, 8, 30, 19, 30, 41, 642024), 'step_id_1': '801ea7259f504426'}]\n(Background on this error at: https://sqlalche.me/e/20/e3q8)",
    "trace writer flush failed: (pymysql.err.OperationalError) (1213, 'Deadlock found when trying to get lock; try restarting transaction')\n[SQL: UPDATE trace_step SET status=%(status)s, completed_at=%(completed_at)s WHERE trace_step.step_id = %(step_id_1)s]\n[parameters: {'status': 'completed', 'completed_at': datetime.datetime(2026, 8, 30, 19, 30, 25, 470486), 'step_id_1': 'ee20c822499e42a3'}]\n(Background on this error at: https://sqlalche.me/e/20/e3q8)",
    "trace writer flush failed: (pymysql.err.OperationalError) (1213, 'Deadlock found when trying to get lock; try restarting transaction')\n[SQL: UPDATE trace_step SET status=%(status)s, completed_at=%(completed_at)s WHERE trace_step.step_id = %(step_id_1)s]\n[parameters: {'status': 'completed', 'completed_at': datetime.datetime(2026, 8, 30, 19, 29, 25, 941548), 'step_id_1': '43878818290741e6'}]\n(Background on this error at: https://sqlalche.me/e/20/e3q8)",
    "trace writer flush failed: (pymysql.err.OperationalError) (1213, 'Deadlock found when trying to get lock; try restarting transaction')\n[SQL: UPDATE trace_step SET status=%(status)s, completed_at=%(completed_at)s WHERE trace_step.step_id = %(step_id_1)s]\n[parameters: {'status': 'completed', 'completed_at': datetime.datetime(2026, 8, 30, 17, 42, 54, 345850), 'step_id_1': 'e5084f4f51d84760'}]\n(Background on this error at: https://sqlalche.me/e/20/e3q8)",
]

REQUEST_ANOMALY = [
    "PUT /api/view/shares/ChFICzP9VHlILNzd2xEdUKC3ejuD4iOh/",
    "PUT /api/view/shares/VHppTliH5Pr97ZJ9Tk3f3LorG5UDjuL3/",
    "PUT /api/view/shares/BS1jNwZnb05aPP864wDgzbEoZET1ufGc/",
    "PUT /api/view/shares/ehpjDGXiHDN2MOOI8tWftGoHPtGk0Bmg/",
    "PUT /api/view/shares/xXjcevd8ku7GO7dBbUdiOJ6tzIS8PF2t/",
    "PUT /api/view/shares/WNseyWe9XHmqblI0Vz8fzWkt2uQNEsKu/",
    "PUT /api/view/shares/A2u1CnKmbr1AP3PjfKi02mRD6oTKKXPw/",
    "GET /api/view/shares/WNseyWe9XHmqblI0Vz8fzWkt2uQNEsKu/",
    "PUT /api/view/shares/sZjEc3IAkWPXMui9WNZD7rBOcjVStfAc/",
    "PUT /api/view/shares/Ihf3tRcUJ0bJ0ltXajOiFMpzXOLfYuov/",
    "PUT /api/view/shares/NSsU6I3Flj09eydanjXULo4zguIKJM4P/",
    "PUT /api/view/shares/34Ngeaq7M4tMz2tttFwUq4yUF2YSeqvT/",
]

TASK_FAILURE = [
    "Work queue backlog is stale \u2014 3 pending item(s) overdue by more than 600s and unclaimed; dispatch is not draining the queue",
    "Work queue backlog is stale \u2014 1 pending item(s) overdue by more than 600s and unclaimed; dispatch is not draining the queue",
    "Task failure: adp_import",
    "Transient failure (no retry policy): ExpiredAssignmentSweeper.periodic",
    "Transient failure (no retry policy): MissingRecipientAssignmentSweeper.periodic",
    "Transient failure (no retry policy): StaleIdentityProgressSweeper.periodic",
]

CLIENT_CRASH = [
    "AxiosError: Request failed with status code 409",
    "AxiosError: Network Error",
    "AxiosError: timeout of 12000ms exceeded",
    "TypeError: Failed to fetch",
    "AxiosError: No organization or network context selected",
    "TypeError: Importing a module script failed.",
]

SINGLETONS = [
    "Recurring invitation dropped stale contact-method overrides",
    "No usable thumbnail found",
    "Scheduled workflow not found: CloudWatchMetricSweeper.periodic",
    "StaleDurationSweeper: computed duration exceeds the storable maximum; rows skipped as non-convergent \u2014 repair the source component durations",
    "GET /api/auth/social/callback/google/",
]


def _signatures(messages, *, category, source=None, environment="production", params=None):
    return {
        condition_signature(
            category, source, environment, m if params else normalize_message(m)
        )
        for m in messages
    }


class TestProductionFixtures:
    """A1 — the live categories, graded on the live messages."""

    def test_twelve_deadlock_reports_are_one_condition(self):
        """The 12 prod trace-writer deadlocks are one problem, not twelve.

        They differ only below the first line: the SQL, the bound parameters
        (a datetime and a 16-char hex step id). One deadlock condition is what
        an operator can acknowledge; twelve is a wall.
        """
        assert len(MANAGER_FAILURE) == 12
        assert len(_signatures(MANAGER_FAILURE, category="manager_failure")) == 1

    def test_deadlock_identity_is_the_first_line(self):
        template = normalize_message(MANAGER_FAILURE[0])
        assert "\n" not in template
        assert template.startswith("trace writer flush failed:")
        # The per-occurrence noise is gone: no SQL, no bound parameters.
        assert "step_id" not in template
        assert "UPDATE trace_step" not in template

    def test_work_queue_backlog_count_variants_merge(self):
        """"3 pending item(s)" and "1 pending item(s)" are one backlog."""
        backlog = [m for m in TASK_FAILURE if m.startswith("Work queue backlog")]
        assert len(backlog) == 2
        assert len(_signatures(backlog, category="task_failure")) == 1

    def test_backlog_and_task_failure_stay_separate(self):
        """A stale queue and a failed ADP import are not the same problem."""
        backlog = next(m for m in TASK_FAILURE if m.startswith("Work queue backlog"))
        adp = next(m for m in TASK_FAILURE if m.startswith("Task failure:"))
        assert len(_signatures([backlog, adp], category="task_failure")) == 2

    def test_distinct_exceptions_in_one_category_stay_distinct(self):
        """Two different client crashes in ``client_crash`` are two conditions."""
        network = next(m for m in CLIENT_CRASH if m == "AxiosError: Network Error")
        conflict = next(m for m in CLIENT_CRASH if "409" in m)
        assert len(_signatures([network, conflict], category="client_crash")) == 2

    def test_timeout_variants_merge_on_the_duration(self):
        """"timeout of 12000ms" and "timeout of 30000ms" are one condition."""
        real = next(m for m in CLIENT_CRASH if "timeout of" in m)
        variant = real.replace("12000ms", "30000ms")
        assert len(_signatures([real, variant], category="client_crash")) == 1

    def test_sweeper_class_names_are_not_collapsed(self):
        """Three different sweepers failing are three problems.

        The class name is not noise — it is the whole diagnosis. A normalizer
        eager enough to merge these would hide two broken sweepers behind one
        acknowledged row.
        """
        transient = [m for m in TASK_FAILURE if m.startswith("Transient failure")]
        assert len(transient) == 3
        assert len(_signatures(transient, category="task_failure")) == 3

    def test_every_live_singleton_message_normalizes_stably(self):
        """The one-off prod categories survive a round trip unchanged in count."""
        for message in SINGLETONS:
            template = normalize_message(message)
            assert template
            assert normalize_message(template) == template


class TestRequestAnomalyIdentity:
    """The stub case — why ``params`` exists.

    ``request_anomaly`` messages carry a 32-char opaque share stub. It is not
    a number, not a uuid and not hex, and guessing at it from the text would
    mean guessing at every url-shaped token in every message. The emit site
    passes ``params`` instead, and identity becomes exact.
    """

    def test_stubs_do_not_merge_under_the_text_normalizer(self):
        puts = [m for m in REQUEST_ANOMALY if m.startswith("PUT ")]
        assert len(puts) == 11
        assert len(_signatures(puts, category="request_anomaly")) == 11

    def test_params_collapse_every_stub_to_one_route_condition(self):
        """With a template, all 11 PUTs are one condition and the GET is another."""
        template_for = lambda m: f"{m.split()[0]} /api/view/shares/{{stub}}/"  # noqa: E731
        put_sigs = _signatures(
            [template_for(m) for m in REQUEST_ANOMALY if m.startswith("PUT ")],
            category="request_anomaly",
            params=True,
        )
        all_sigs = _signatures(
            [template_for(m) for m in REQUEST_ANOMALY],
            category="request_anomaly",
            params=True,
        )
        assert len(put_sigs) == 1
        assert len(all_sigs) == 2  # PUT and GET are different problems


class TestSignatureScope:
    """P6/P9 — what else identity depends on."""

    def test_environment_splits_the_signature(self):
        """The same failure in staging and production is two conditions.

        Otherwise resolving the staging copy silences production — the one
        mistake this table exists to prevent.
        """
        template = normalize_message(MANAGER_FAILURE[0])
        staging = condition_signature("manager_failure", None, "staging", template)
        production = condition_signature("manager_failure", None, "production", template)
        assert staging != production

    def test_category_and_source_split_the_signature(self):
        assert condition_signature("a", "s", "prod", "boom") != condition_signature(
            "b", "s", "prod", "boom"
        )
        assert condition_signature("a", "s1", "prod", "boom") != condition_signature(
            "a", "s2", "prod", "boom"
        )

    def test_field_boundaries_cannot_be_forged(self):
        """No arrangement of separators makes two different tuples collide."""
        assert condition_signature("a|b", "c", "prod", "t") != condition_signature(
            "a", "b|c", "prod", "t"
        )

    def test_signature_is_a_64_char_hex_key(self):
        key = condition_signature("cat", None, "production", "boom")
        assert len(key) == 64
        assert key == condition_signature("cat", None, "production", "boom")

    def test_signature_ignores_template_beyond_the_column_width(self):
        """Identity cannot depend on bytes the column will not store."""
        base = "x" * 500
        assert condition_signature("c", None, "e", base) == condition_signature(
            "c", None, "e", base + "tail"
        )


class TestNormalizerUnits:
    """The individual replacements, stated as rules."""

    def test_numbers_uuids_hex_quoted_and_timestamps_become_placeholders(self):
        assert normalize_message("failed 42 times") == "failed <n> times"
        assert (
            normalize_message("job 550e8400-e29b-41d4-a716-446655440000 died")
            == "job <uuid> died"
        )
        assert normalize_message("step 08f16cbfcbbb456a stuck") == "step <hex> stuck"
        assert normalize_message("could not find 'widget-7'") == "could not find <str>"
        assert (
            normalize_message("expired at 2026-08-30T20:49:56.843067+00:00")
            == "expired at <ts>"
        )

    def test_ordinary_words_are_left_alone(self):
        """Hex-looking English must not be mistaken for an id.

        ``deadbeefed`` is eleven characters of pure hex; requiring a digit as
        well is what keeps English out of the placeholder.
        """
        assert normalize_message("the deadbeefed accessed feedbacc") == (
            "the deadbeefed accessed feedbacc"
        )

    def test_a_long_run_of_digits_is_a_number_not_a_hex_id(self):
        assert normalize_message("id 123456789 failed") == "id <n> failed"

    def test_normalizing_a_template_is_a_no_op(self):
        once = normalize_message("row 42 of 'batch-9' failed at 2026-08-30 10:00:00")
        assert normalize_message(once) == once

    def test_whitespace_runs_collapse(self):
        assert normalize_message("a   b") == normalize_message("a b")

    def test_empty_message_is_survivable(self):
        assert normalize_message("") == ""
