"""Validate LLM-written SQL BEFORE it touches the database (safety layer 2 and 3).

validate_and_limit(sql) either returns a normalised, LIMIT-capped SELECT or raises SqlRejected
with a message that is safe to show to the user AND to feed back to the LLM for a retry.

Rules (all enforced on the parsed tree, never with string matching on the raw text):
  1. exactly ONE statement
  2. it is a SELECT (a WITH ... SELECT, or a UNION/INTERSECT/EXCEPT of SELECTs): no DDL/DML, no SELECT INTO, no FOR UPDATE
  3. every table is schema-qualified analytics.<name> (CTE names excepted); no other schema, no table functions
  4. no function outside a small allow-list of ones sqlglot does not already know as harmless built-ins
     (pg_sleep, set_config, pg_read_file, lo_import, dblink, ... all parse as "Anonymous" and are refused)
  5. LIMIT is forced to at most MAX_LIMIT
"""
from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

ALLOWED_SCHEMA = "analytics"
MAX_LIMIT = 200

# Statement kinds that must never appear anywhere in the tree (also catches data-modifying CTEs).
_FORBIDDEN_NODES = tuple(
    getattr(exp, name)
    for name in (
        "Insert", "Update", "Delete", "Merge", "Drop", "Create", "Alter", "AlterTable", "TruncateTable",
        "Command", "Copy", "Grant", "Revoke", "Set", "Into", "Lock", "Pragma", "Use", "Transaction",
        "Commit", "Rollback", "Analyze", "Describe", "Show", "Kill", "Declare", "Call", "Execute",
        "GenerateSeries", "Unnest", "Values",   # row generators: cheap DoS / useless for our marts
    )
    if hasattr(exp, name)
)

# Functions sqlglot parses as exp.Anonymous (it has no typed node for them) that are still harmless.
_SAFE_ANONYMOUS_FUNCTIONS = frozenset({
    "date_part", "to_char", "trunc", "age", "date_bin", "initcap", "split_part", "string_agg",
    "array_agg", "percentile_cont", "percentile_disc", "mode", "regr_slope", "corr", "ntile",
    "cume_dist", "percent_rank", "dense_rank", "rank", "row_number", "first_value", "last_value",
    "nth_value", "lag", "lead", "char_length", "greatest", "least", "div", "mod", "sign", "cbrt",
    "stddev_pop", "stddev_samp", "var_pop", "var_samp", "bool_and", "bool_or", "every",
})

_SET_OPERATIONS = tuple(getattr(exp, n) for n in ("SetOperation", "Union", "Intersect", "Except") if hasattr(exp, n))


class SqlRejected(ValueError):
    """The SQL is not allowed. str(exc) explains why."""


def _reject(msg: str):
    raise SqlRejected(msg)


def validate_and_limit(sql: str, allowed_tables: set[str] | None = None) -> str:
    """Return safe SQL (Postgres dialect, LIMIT <= MAX_LIMIT) or raise SqlRejected.

    allowed_tables: optional set of table names (without schema) that exist in `analytics`.
    """
    if not sql or not sql.strip():
        _reject("empty SQL")

    try:
        statements = [s for s in sqlglot.parse(sql, dialect="postgres") if s is not None]
    except SqlglotError as exc:
        _reject(f"SQL could not be parsed: {str(exc).splitlines()[0][:200]}")
    if len(statements) != 1:
        _reject(f"exactly one SELECT statement is allowed, got {len(statements)}")
    stmt = statements[0]

    if not isinstance(stmt, (exp.Select, *_SET_OPERATIONS)):
        _reject(f"only SELECT statements are allowed, got {type(stmt).__name__.upper()}")

    for node in stmt.walk():
        if isinstance(node, _FORBIDDEN_NODES):
            _reject(f"{type(node).__name__.upper()} is not allowed in a read-only query")

    cte_names = {cte.alias_or_name.lower() for cte in stmt.find_all(exp.CTE)}
    for table in stmt.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            _reject("table-valued functions are not allowed")
        name, schema = table.name.lower(), table.db.lower()
        if table.catalog:
            _reject("cross-database references are not allowed")
        if not schema:
            if name in cte_names:
                continue
            _reject(f"table '{name}' must be schema-qualified as {ALLOWED_SCHEMA}.{name}")
        if schema != ALLOWED_SCHEMA:
            _reject(f"only the '{ALLOWED_SCHEMA}' schema may be queried, got '{schema}.{name}'")
        if allowed_tables is not None and name not in allowed_tables:
            _reject(f"unknown table {ALLOWED_SCHEMA}.{name}; available: {', '.join(sorted(allowed_tables))}")

    for fn in stmt.find_all(exp.Anonymous):
        if fn.name.lower() not in _SAFE_ANONYMOUS_FUNCTIONS:
            _reject(f"function '{fn.name}' is not allowed")

    # Layer 3: force LIMIT. Keep a smaller literal limit, replace anything else (missing, huge, FETCH FIRST, expression).
    limit = stmt.args.get("limit")
    keep = False
    if isinstance(limit, exp.Limit) and isinstance(limit.expression, exp.Literal) and not limit.expression.is_string:
        try:
            keep = int(limit.expression.name) <= MAX_LIMIT
        except ValueError:
            keep = False
    if not keep:
        stmt.set("limit", exp.Limit(expression=exp.Literal.number(MAX_LIMIT)))

    return stmt.sql(dialect="postgres", comments=False)
