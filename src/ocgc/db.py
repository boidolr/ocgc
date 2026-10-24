"""SQLite query layer for OpenCode's database."""

import contextlib
import json
import os
import sqlite3
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

DEFAULT_DB_PATH = Path.home() / ".local" / "share" / "opencode" / "opencode.db"


@dataclass
class DBInfo:
    path: Path
    db_size: int
    wal_size: int

    @property
    def total_size(self) -> int:
        return self.db_size + self.wal_size


@dataclass
class SessionRow:
    id: str
    parent_id: str | None
    directory: str
    title: str | None
    time_created: int  # ms epoch
    time_updated: int  # ms epoch
    size_bytes: int
    message_count: int

    @property
    def is_subagent(self) -> bool:
        return self.parent_id is not None


@dataclass
class PartTypeStats:
    type_name: str
    count: int
    size_bytes: int


@dataclass
class FilesystemStats:
    session_diff_size: int
    snapshot_size: int
    tool_output_size: int
    log_size: int = 0
    session_diff_count: int = 0
    snapshot_count: int = 0

    @property
    def total_size(self) -> int:
        return self.session_diff_size + self.snapshot_size + self.tool_output_size + self.log_size


@dataclass
class PurgeFilesResult:
    files_deleted: int = 0
    bytes_freed: int = 0


@dataclass
class OrphanDiff:
    session_id: str
    path: Path
    size: int


def get_db_path() -> Path:
    return Path(os.environ.get("OCGC_DB_PATH", str(DEFAULT_DB_PATH)))


def check_opencode_running() -> bool:
    if os.environ.get("OCGC_SKIP_RUNNING_CHECK"):
        return False
    try:
        result = subprocess.run(
            ["pgrep", "-x", "opencode"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def has_table(conn: sqlite3.Connection, name: str) -> bool:
    """Return True if the main database has a table with the given name."""
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)).fetchone() is not None


_SESSION_COLUMNS = "id, parent_id, directory, title, time_created, time_updated"

# Empty placeholder for LEFT JOINs / views when an optional table doesn't exist.
_EMPTY_SESSION_ID_SIZE = "SELECT NULL AS session_id, NULL AS size_bytes WHERE 0"
_EMPTY_SESSION_ID_COUNT = "SELECT NULL AS session_id, NULL AS msg_count WHERE 0"
_EMPTY_STATS = "SELECT NULL AS session_id, NULL AS size_bytes, NULL AS message_count WHERE 0"


def _create_unified_views(conn: sqlite3.Connection) -> None:
    """Create temp views unifying OpenCode's v1 and v2 schemas.

    v2 tables (session_v2/session_message) coexist with v1 tables (session/part/message).
    A session id can be present in both during migration (the v1 rows are stale
    duplicates), so the unified session view prefers the v2 row. Per-session stats
    likewise come from the v2 branch whenever the id exists in session_v2. Branches
    whose tables don't exist are omitted so older/newer databases still work.
    """
    has_v1 = has_table(conn, "session")
    has_v2 = has_table(conn, "session_v2")

    session_selects = []
    if has_v2:
        session_selects.append(f"SELECT {_SESSION_COLUMNS} FROM session_v2")
    if has_v1:
        v1_select = f"SELECT {_SESSION_COLUMNS} FROM session"
        if has_v2:
            v1_select += " WHERE id NOT IN (SELECT id FROM session_v2)"
        session_selects.append(v1_select)
    if not session_selects:
        session_selects.append(
            "SELECT NULL AS id, NULL AS parent_id, NULL AS directory, NULL AS title,"
            " NULL AS time_created, NULL AS time_updated WHERE 0"
        )
    conn.execute("CREATE TEMP VIEW ocgc_session AS " + " UNION ALL ".join(session_selects))

    stats_selects = []
    if has_v2 and has_table(conn, "session_message"):
        stats_selects.append("""
            SELECT
                session_id,
                COALESCE(SUM(LENGTH(data)), 0) AS size_bytes,
                COALESCE(SUM(CASE WHEN type IN ('user', 'assistant') THEN 1 ELSE 0 END), 0) AS message_count
            FROM session_message
            GROUP BY session_id
        """)
    if has_v1:
        part_subq = (
            "SELECT session_id, SUM(LENGTH(data)) AS size_bytes FROM part GROUP BY session_id"
            if has_table(conn, "part")
            else _EMPTY_SESSION_ID_SIZE
        )
        msg_subq = (
            "SELECT session_id, COUNT(*) AS msg_count FROM message GROUP BY session_id"
            if has_table(conn, "message")
            else _EMPTY_SESSION_ID_COUNT
        )
        not_in_v2 = " WHERE s.id NOT IN (SELECT id FROM session_v2)" if has_v2 else ""
        stats_selects.append(f"""
            SELECT
                s.id AS session_id,
                COALESCE(ps.size_bytes, 0) AS size_bytes,
                COALESCE(mc.msg_count, 0) AS message_count
            FROM session s
            LEFT JOIN ({part_subq}) ps ON ps.session_id = s.id
            LEFT JOIN ({msg_subq}) mc ON mc.session_id = s.id{not_in_v2}
        """)
    if not stats_selects:
        stats_selects.append(_EMPTY_STATS)
    conn.execute("CREATE TEMP VIEW ocgc_session_stats AS " + " UNION ALL ".join(stats_selects))


def connect(readonly: bool = True) -> sqlite3.Connection:
    path = get_db_path()
    if not path.exists():
        raise FileNotFoundError(f"OpenCode database not found at {path}")
    uri = f"file:{quote(str(path))}{'?mode=ro' if readonly else ''}"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    if not readonly:
        conn.execute("PRAGMA journal_mode=WAL")
    _create_unified_views(conn)
    return conn


def get_db_info() -> DBInfo:
    path = get_db_path()
    wal_path = Path(str(path) + "-wal")
    return DBInfo(
        path=path,
        db_size=path.stat().st_size if path.exists() else 0,
        wal_size=wal_path.stat().st_size if wal_path.exists() else 0,
    )


def get_session_count(conn: sqlite3.Connection) -> tuple[int, int]:
    """Return (root_count, subagent_count) over the unified (deduped) session set."""
    row = conn.execute("""
        SELECT
            SUM(CASE WHEN parent_id IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN parent_id IS NOT NULL THEN 1 ELSE 0 END)
        FROM ocgc_session
    """).fetchone()
    return (row[0] or 0, row[1] or 0)


def _merge_part_type_rows(
    stats: dict[str, list[int]], rows: list[sqlite3.Row], text_row: sqlite3.Row | None = None
) -> None:
    for r in rows:
        entry = stats.setdefault(r[0] or "unknown", [0, 0])
        entry[0] += r[1] or 0
        entry[1] += r[2] or 0
    if text_row is not None:
        entry = stats.setdefault("text", [0, 0])
        entry[0] += text_row[0] or 0
        entry[1] += text_row[1] or 0


def _part_type_stats(conn: sqlite3.Connection, session_filter: str | None) -> dict[str, list[int]]:
    """Aggregate part-type storage across v1 part rows and v2 content items.

    Returns a mapping of part type -> [count, size_bytes]. When session_filter is
    given it is a SQL condition on the unified session table aliased as `s`.
    """
    stats: dict[str, list[int]] = {}

    if has_table(conn, "part"):
        if session_filter is None:
            rows = conn.execute("""
                SELECT
                    json_extract(data, '$.type') AS t,
                    COUNT(*) AS cnt,
                    SUM(LENGTH(data)) AS sz
                FROM part
                GROUP BY t
            """).fetchall()
            _merge_part_type_rows(stats, rows)
        else:
            rows = conn.execute(f"""
                SELECT
                    json_extract(p.data, '$.type') AS t,
                    COUNT(*) AS cnt,
                    SUM(LENGTH(p.data)) AS sz
                FROM part p
                JOIN ocgc_session s ON s.id = p.session_id
                WHERE {session_filter}
                GROUP BY t
            """).fetchall()
            _merge_part_type_rows(stats, rows)

    if has_table(conn, "session_message"):
        # v2 assistant messages embed parts in data.$.content[]
        if session_filter is None:
            rows = conn.execute("""
                SELECT
                    -- je.value for scalar items can be a bare (non-JSON) string on
                    -- newer SQLite; only object items carry $.type, so guard json_extract.
                    CASE WHEN je.type = 'object' THEN json_extract(je.value, '$.type') END AS t,
                    COUNT(*) AS cnt,
                    SUM(LENGTH(je.value)) AS sz
                FROM session_message m, json_each(m.data, '$.content') je
                GROUP BY t
            """).fetchall()
            text_row = conn.execute("""
                SELECT COUNT(*), SUM(LENGTH(json_extract(data, '$.text')))
                FROM session_message
                WHERE type = 'user' AND json_extract(data, '$.text') IS NOT NULL
            """).fetchone()
        else:
            rows = conn.execute(f"""
                SELECT
                    -- je.value for scalar items can be a bare (non-JSON) string on
                    -- newer SQLite; only object items carry $.type, so guard json_extract.
                    CASE WHEN je.type = 'object' THEN json_extract(je.value, '$.type') END AS t,
                    COUNT(*) AS cnt,
                    SUM(LENGTH(je.value)) AS sz
                FROM session_message m
                JOIN ocgc_session s ON s.id = m.session_id, json_each(m.data, '$.content') je
                WHERE {session_filter}
                GROUP BY t
            """).fetchall()
            text_row = conn.execute(f"""
                SELECT COUNT(*), SUM(LENGTH(json_extract(m.data, '$.text')))
                FROM session_message m
                JOIN ocgc_session s ON s.id = m.session_id
                WHERE {session_filter}
                  AND m.type = 'user' AND json_extract(m.data, '$.text') IS NOT NULL
            """).fetchone()
        _merge_part_type_rows(stats, rows, text_row=text_row)

    return stats


def _to_sorted_part_stats(stats: dict[str, list[int]]) -> list[PartTypeStats]:
    return sorted(
        (PartTypeStats(type_name, count, size) for type_name, (count, size) in stats.items()),
        key=lambda s: s.size_bytes,
        reverse=True,
    )


def get_part_type_stats(conn: sqlite3.Connection) -> list[PartTypeStats]:
    return _to_sorted_part_stats(_part_type_stats(conn, None))


def get_age_distribution(conn: sqlite3.Connection, now_ms: int) -> dict[str, int]:
    """Return session counts bucketed by age."""
    buckets = {
        "last 24h": 24 * 3600 * 1000,
        "1-7 days": 7 * 24 * 3600 * 1000,
        "7-14 days": 14 * 24 * 3600 * 1000,
        "14-30 days": 30 * 24 * 3600 * 1000,
    }
    result: dict[str, int] = {}
    prev_cutoff = now_ms
    for label, age_ms in buckets.items():
        cutoff = now_ms - age_ms
        count = conn.execute(
            "SELECT COUNT(*) FROM ocgc_session WHERE time_created < ? AND time_created >= ?",
            (prev_cutoff, cutoff),
        ).fetchone()[0]
        result[label] = count
        prev_cutoff = cutoff

    older = conn.execute(
        "SELECT COUNT(*) FROM ocgc_session WHERE time_created < ?",
        (prev_cutoff,),
    ).fetchone()[0]
    result["older than 30d"] = older
    return result


def get_sessions(
    conn: sqlite3.Connection,
    sort_by: str = "size",
    limit: int | None = None,
) -> list[SessionRow]:
    order_clause = {
        "size": "size_bytes DESC",
        "age": "s.time_created ASC",
        "name": "s.title ASC",
    }.get(sort_by, "size_bytes DESC")

    limit_clause = "LIMIT ?" if limit else ""
    params: list[int] = []
    if limit:
        params.append(limit)

    rows = conn.execute(
        f"""
        SELECT
            s.id,
            s.parent_id,
            s.directory,
            s.title,
            s.time_created,
            s.time_updated,
            COALESCE(st.size_bytes, 0) AS size_bytes,
            COALESCE(st.message_count, 0) AS message_count
        FROM ocgc_session s
        LEFT JOIN ocgc_session_stats st ON st.session_id = s.id
        ORDER BY {order_clause}
        {limit_clause}
    """,
        params,
    ).fetchall()

    return [
        SessionRow(
            id=r["id"],
            parent_id=r["parent_id"],
            directory=r["directory"],
            title=r["title"],
            time_created=r["time_created"],
            time_updated=r["time_updated"],
            size_bytes=r["size_bytes"],
            message_count=r["message_count"],
        )
        for r in rows
    ]


def get_part_type_stats_by_session_type(
    conn: sqlite3.Connection,
) -> tuple[list[PartTypeStats], list[PartTypeStats]]:
    """Return (root_stats, subagent_stats)."""
    root = _to_sorted_part_stats(_part_type_stats(conn, "s.parent_id IS NULL"))
    sub = _to_sorted_part_stats(_part_type_stats(conn, "s.parent_id IS NOT NULL"))
    return root, sub


def get_growth_rate(conn: sqlite3.Connection) -> float | None:
    """Estimate MB per active day of usage."""
    row = conn.execute("""
        SELECT COUNT(DISTINCT date(time_created / 1000, 'unixepoch')) AS active_days
        FROM ocgc_session
    """).fetchone()
    if not row or not row["active_days"] or row["active_days"] < 1:
        return None
    active_days: int = row["active_days"]
    # ocgc_session_stats already implements v2-preferred dedup, so stale v1
    # copies of migrated sessions are not double-counted here.
    total_bytes = int(conn.execute("SELECT COALESCE(SUM(size_bytes), 0) FROM ocgc_session_stats").fetchone()[0])
    return (total_bytes / 1048576) / active_days


def get_session_ids_for_purge(
    conn: sqlite3.Connection,
    older_than_ms: int | None = None,
    subagents_only: bool = False,
    larger_than_bytes: int | None = None,
    session_ids: list[str] | None = None,
    keep_latest: int | None = None,
    now_ms: int | None = None,
) -> list[str]:
    """Return session IDs matching purge criteria. Filters combine with AND."""
    conditions = []
    params: list[str | int] = []

    if session_ids:
        placeholders = ",".join("?" for _ in session_ids)
        conditions.append(f"s.id IN ({placeholders})")
        params.extend(session_ids)

    if subagents_only:
        conditions.append("s.parent_id IS NOT NULL")

    if older_than_ms is not None and now_ms is not None:
        cutoff = now_ms - older_than_ms
        conditions.append("s.time_created < ?")
        params.append(cutoff)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    if larger_than_bytes is not None:
        query = f"""
            SELECT s.id FROM ocgc_session s
            LEFT JOIN ocgc_session_stats st ON st.session_id = s.id
            {where}
            {"AND" if conditions else "WHERE"} COALESCE(st.size_bytes, 0) >= ?
        """
        params.append(larger_than_bytes)
    else:
        query = f"SELECT s.id FROM ocgc_session s {where}"

    ids = {r[0] for r in conn.execute(query, params)}

    if keep_latest is not None:
        if not conditions and larger_than_bytes is None and not session_ids:
            # keep_latest alone: delete everything except the latest N
            latest_ids = {
                r[0]
                for r in conn.execute(
                    "SELECT id FROM ocgc_session ORDER BY time_created DESC LIMIT ?",
                    (keep_latest,),
                )
            }
            all_ids = {r[0] for r in conn.execute("SELECT id FROM ocgc_session")}
            ids = all_ids - latest_ids
        else:
            # With other filters: compute latest N from the already-filtered set
            if ids:
                placeholders = ",".join("?" for _ in ids)
                id_list = list(ids)
                latest_ids = {
                    r[0]
                    for r in conn.execute(
                        f"SELECT id FROM ocgc_session WHERE id IN ({placeholders}) ORDER BY time_created DESC LIMIT ?",
                        [*id_list, keep_latest],
                    )
                }
                ids -= latest_ids

    return sorted(ids)


def get_purge_summary(conn: sqlite3.Connection, session_ids: list[str]) -> dict[str, int]:
    """Return summary stats for sessions about to be purged (both v1 and v2 copies)."""
    if not session_ids:
        return {"session_count": 0, "part_count": 0, "message_count": 0, "total_bytes": 0}
    placeholders = ",".join("?" for _ in session_ids)

    part_count = 0
    total_bytes = 0
    if has_table(conn, "part"):
        row = conn.execute(
            f"SELECT COUNT(*), COALESCE(SUM(LENGTH(data)), 0) FROM part WHERE session_id IN ({placeholders})",
            session_ids,
        ).fetchone()
        part_count += int(row[0] or 0)
        total_bytes += int(row[1] or 0)

    message_count = 0
    if has_table(conn, "message"):
        message_count += int(
            conn.execute(
                f"SELECT COUNT(*) FROM message WHERE session_id IN ({placeholders})",
                session_ids,
            ).fetchone()[0]
            or 0
        )

    if has_table(conn, "session_message"):
        total_bytes += int(
            conn.execute(
                f"SELECT COALESCE(SUM(LENGTH(data)), 0) FROM session_message WHERE session_id IN ({placeholders})",
                session_ids,
            ).fetchone()[0]
        )
        message_count += int(
            conn.execute(
                f"SELECT COUNT(*) FROM session_message"
                f" WHERE session_id IN ({placeholders}) AND type IN ('user', 'assistant')",
                session_ids,
            ).fetchone()[0]
            or 0
        )
        part_count += int(
            conn.execute(
                f"""SELECT COUNT(*) FROM session_message m, json_each(m.data, '$.content')
                    WHERE m.session_id IN ({placeholders})""",
                session_ids,
            ).fetchone()[0]
            or 0
        )

    return {
        "session_count": len(session_ids),
        "part_count": part_count,
        "message_count": message_count,
        "total_bytes": total_bytes,
    }


def get_reasoning_summary(conn: sqlite3.Connection, session_ids: list[str] | None) -> dict[str, int]:
    """Return summary stats for reasoning parts in given sessions (or all if None)."""
    if session_ids is not None and not session_ids:
        return {"part_count": 0, "total_bytes": 0}
    part_count = 0
    total_bytes = 0
    if has_table(conn, "part"):
        if session_ids is not None:
            placeholders = ",".join("?" for _ in session_ids)
            row = conn.execute(
                f"""SELECT COUNT(*), COALESCE(SUM(LENGTH(data)), 0) FROM part
                    WHERE json_extract(data, '$.type') = 'reasoning'
                    AND session_id IN ({placeholders})""",
                session_ids,
            ).fetchone()
        else:
            row = conn.execute(
                """SELECT COUNT(*), COALESCE(SUM(LENGTH(data)), 0) FROM part
                   WHERE json_extract(data, '$.type') = 'reasoning'"""
            ).fetchone()
        part_count += int(row[0] or 0)
        total_bytes += int(row[1] or 0)
    if has_table(conn, "session_message"):
        conditions = ["CASE WHEN c.type = 'object' THEN json_extract(c.value, '$.type') END = 'reasoning'"]
        params: list[str] = []
        if session_ids is not None:
            placeholders = ",".join("?" for _ in session_ids)
            conditions.append(f"m.session_id IN ({placeholders})")
            params.extend(session_ids)
        row = conn.execute(
            f"""SELECT COUNT(*), COALESCE(SUM(LENGTH(c.value)), 0)
                FROM session_message m, json_each(m.data, '$.content') c
                WHERE {" AND ".join(conditions)}""",
            params,
        ).fetchone()
        part_count += int(row[0] or 0)
        total_bytes += int(row[1] or 0)
    return {"part_count": part_count, "total_bytes": total_bytes}


def _delete_ids(conn: sqlite3.Connection, table: str, column: str, session_ids: list[str]) -> int:
    """Delete rows by session-keyed column; tolerate missing (older-version) tables."""
    placeholders = ",".join("?" for _ in session_ids)
    try:
        return conn.execute(f"DELETE FROM {table} WHERE {column} IN ({placeholders})", session_ids).rowcount
    except sqlite3.OperationalError as e:
        if "no such table" not in str(e).lower():
            raise
        return 0


def purge_sessions(conn: sqlite3.Connection, session_ids: list[str]) -> PurgeFilesResult:
    """Delete sessions and all satellite rows from both v1 and v2 schemas.

    SQLite FKs are not enforced on these databases, so every dependent table is
    deleted explicitly; _delete_ids tolerates tables that don't exist. Also
    removes the v2 event store (whose aggregate_id is the session id but has no
    FK to sessions) and session_diff files.
    """
    if not session_ids:
        return PurgeFilesResult()
    # v1 schema
    for table, column in (
        ("part", "session_id"),
        ("message", "session_id"),
        ("todo", "session_id"),
        ("session_share", "session_id"),
        ("session", "id"),
    ):
        _delete_ids(conn, table, column, session_ids)
    # v2 schema
    for table in ("session_message", "session_inbox", "session_pending", "instruction_entry", "instruction_state"):
        _delete_ids(conn, table, "session_id", session_ids)
    _delete_ids(conn, "session_v2", "id", session_ids)
    # Event store (no FK to sessions; deleting the session would orphan these rows)
    _delete_ids(conn, "event", "aggregate_id", session_ids)
    _delete_ids(conn, "event_sequence", "aggregate_id", session_ids)
    conn.commit()

    # Best-effort cleanup of session_diff files
    return purge_session_diffs(session_ids)


def strip_reasoning(conn: sqlite3.Connection, session_ids: list[str] | None = None) -> int:
    """Delete v1 reasoning parts and strip v2 embedded reasoning.

    Return the combined count of removed items.
    """
    if session_ids is not None and not session_ids:
        return 0

    count = 0
    if has_table(conn, "part"):
        if session_ids is not None:
            placeholders = ",".join("?" for _ in session_ids)
            cur = conn.execute(
                f"""DELETE FROM part
                    WHERE json_extract(data, '$.type') = 'reasoning'
                    AND session_id IN ({placeholders})""",
                session_ids,
            )
        else:
            cur = conn.execute("DELETE FROM part WHERE json_extract(data, '$.type') = 'reasoning'")
        count += cur.rowcount

    if has_table(conn, "session_message"):
        count += _strip_v2_reasoning(conn, session_ids)

    conn.commit()
    return count


def _strip_v2_reasoning(conn: sqlite3.Connection, session_ids: list[str] | None, chunk_size: int = 250) -> int:
    """Remove reasoning items from session_message content arrays. Return count removed.

    Streams in chunks so memory stays bounded by chunk_size, not corpus size:
    candidate ids are resolved via a json_each/EXISTS predicate (format-coupled
    LIKE prefilter removed), then each chunk's data is loaded, rewritten, updated
    and committed before the next chunk is touched.
    """
    conditions = [
        """
        EXISTS (
            SELECT 1 FROM json_each(data, '$.content') c
            WHERE CASE WHEN c.type = 'object' THEN json_extract(c.value, '$.type') END = 'reasoning'
        )
    """
    ]
    params: list[str] = []
    if session_ids is not None:
        placeholders = ",".join("?" for _ in session_ids)
        conditions.append(f"session_id IN ({placeholders})")
        params.extend(session_ids)

    candidates = [
        r[0]
        for r in conn.execute(f"SELECT id FROM session_message WHERE {' AND '.join(conditions)}", params).fetchall()
    ]

    removed = 0
    for start in range(0, len(candidates), chunk_size):
        batch = candidates[start : start + chunk_size]
        placeholders = ",".join("?" for _ in batch)
        rows = conn.execute(f"SELECT id, data FROM session_message WHERE id IN ({placeholders})", batch).fetchall()
        updates: list[tuple[str, str]] = []
        for row in rows:
            try:
                obj = json.loads(row["data"])
            except (json.JSONDecodeError, TypeError):
                continue
            content = obj.get("content") if isinstance(obj, dict) else None
            if not isinstance(content, list):
                continue
            kept = [item for item in content if not (isinstance(item, dict) and item.get("type") == "reasoning")]
            if len(kept) == len(content):
                continue
            removed += len(content) - len(kept)
            obj["content"] = kept
            updates.append((json.dumps(obj, ensure_ascii=False, separators=(",", ":")), row["id"]))
        if updates:
            conn.executemany("UPDATE session_message SET data = ? WHERE id = ?", updates)
        # Per-chunk commit: no large transaction pinned, processed rows not retained.
        conn.commit()
    return removed


def get_migrated_v1_sessions(conn: sqlite3.Connection) -> list[tuple[str, int]]:
    """Return (id, reclaimable_bytes) for stale v1 copies of sessions migrated to v2.

    A v1 row is only considered migrated when the id also exists in session_v2
    AND has at least one v2 session_message row (i.e. v2 actually holds data).
    """
    if not has_table(conn, "session") or not has_table(conn, "session_v2") or not has_table(conn, "session_message"):
        return []
    rows = conn.execute("""
        SELECT s.id
        FROM session s
        WHERE EXISTS (SELECT 1 FROM session_v2 v WHERE v.id = s.id)
          AND EXISTS (SELECT 1 FROM session_message sm WHERE sm.session_id = s.id)
    """).fetchall()
    ids = [r[0] for r in rows]
    if not ids:
        return []

    placeholders = ",".join("?" for _ in ids)
    part_bytes: dict[str, int] = {}
    msg_bytes: dict[str, int] = {}
    if has_table(conn, "part"):
        part_bytes = {
            r[0]: int(r[1] or 0)
            for r in conn.execute(
                f"SELECT session_id, SUM(LENGTH(data)) FROM part"
                f" WHERE session_id IN ({placeholders}) GROUP BY session_id",
                ids,
            ).fetchall()
        }
    if has_table(conn, "message"):
        msg_bytes = {
            r[0]: int(r[1] or 0)
            for r in conn.execute(
                f"SELECT session_id, SUM(LENGTH(data)) FROM message"
                f" WHERE session_id IN ({placeholders}) GROUP BY session_id",
                ids,
            ).fetchall()
        }
    return [(sid, part_bytes.get(sid, 0) + msg_bytes.get(sid, 0)) for sid in ids]


def clean_migrated_v1(conn: sqlite3.Connection, ids: list[str]) -> None:
    """Delete stale v1 rows (session/message/part only) for ids that live on in v2.

    Deliberately does not touch todo/session_share (keyed by an id that is still
    live in v2), v2 tables, or session_diff files.
    """
    if not ids:
        return
    for table, column in (("part", "session_id"), ("message", "session_id"), ("session", "id")):
        _delete_ids(conn, table, column, ids)
    conn.commit()


def _total_db_size(path: Path) -> int:
    """Return combined size of db + WAL + SHM files."""
    total = path.stat().st_size if path.exists() else 0
    for suffix in ("-wal", "-shm"):
        p = Path(str(path) + suffix)
        if p.exists():
            total += p.stat().st_size
    return total


def get_storage_dir() -> Path:
    """Return the opencode storage base dir (parent of the DB file)."""
    return get_db_path().parent


def _dir_size(path: Path) -> int:
    """Return total size of all files in a directory tree."""
    if not path.is_dir():
        return 0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def get_log_dir() -> Path:
    """Return the opencode log directory."""
    return get_storage_dir() / "log"


def get_log_files() -> list[tuple[Path, int]]:
    """Return (path, size_bytes) for each file inside the log directory."""
    log_dir = get_log_dir()
    if not log_dir.is_dir():
        return []
    return [(f, f.stat().st_size) for f in sorted(log_dir.rglob("*")) if f.is_file()]


def purge_logs() -> PurgeFilesResult:
    """Delete files inside the log directory, then any now-empty subdirectories.

    The log directory itself is kept. Best-effort.
    """
    result = PurgeFilesResult()
    for path, size in get_log_files():
        try:
            path.unlink()
            result.files_deleted += 1
            result.bytes_freed += size
        except FileNotFoundError:
            pass
    log_dir = get_log_dir()
    if log_dir.is_dir():
        # Deepest first so children are removed before their parents.
        for sub in sorted((p for p in log_dir.rglob("*") if p.is_dir()), reverse=True):
            if not any(sub.iterdir()):
                with contextlib.suppress(OSError):
                    sub.rmdir()
    return result


def get_filesystem_stats() -> FilesystemStats:
    """Return sizes of session_diff/, snapshot/, tool-output/, and log/ directories."""
    base = get_storage_dir()
    diff_dir = base / "storage" / "session_diff"
    snap_dir = base / "snapshot"
    tool_dir = base / "tool-output"

    diff_count = len(list(diff_dir.glob("*.json"))) if diff_dir.is_dir() else 0
    snap_count = len([d for d in snap_dir.iterdir() if d.is_dir()]) if snap_dir.is_dir() else 0

    return FilesystemStats(
        session_diff_size=_dir_size(diff_dir),
        snapshot_size=_dir_size(snap_dir),
        tool_output_size=_dir_size(tool_dir),
        log_size=_dir_size(get_log_dir()),
        session_diff_count=diff_count,
        snapshot_count=snap_count,
    )


def purge_session_diffs(session_ids: list[str]) -> PurgeFilesResult:
    """Delete session_diff JSON files for given session IDs. Best-effort."""
    diff_dir = get_storage_dir() / "storage" / "session_diff"
    result = PurgeFilesResult()
    if not diff_dir.is_dir():
        return result
    for sid in session_ids:
        path = diff_dir / f"{sid}.json"
        try:
            size = path.stat().st_size
            path.unlink()
            result.files_deleted += 1
            result.bytes_freed += size
        except FileNotFoundError:
            pass
    return result


def get_orphan_session_diffs(conn: sqlite3.Connection) -> list[OrphanDiff]:
    """Find session_diff files that have no matching session in either schema."""
    diff_dir = get_storage_dir() / "storage" / "session_diff"
    if not diff_dir.is_dir():
        return []

    db_ids = {r[0] for r in conn.execute("SELECT id FROM ocgc_session")}
    orphans = []
    for path in diff_dir.glob("*.json"):
        sid = path.stem
        if sid not in db_ids:
            orphans.append(OrphanDiff(session_id=sid, path=path, size=path.stat().st_size))
    return sorted(orphans, key=lambda o: o.size, reverse=True)


def purge_orphan_diffs(orphans: list[OrphanDiff]) -> PurgeFilesResult:
    """Delete orphan session_diff files. Best-effort."""
    result = PurgeFilesResult()
    for o in orphans:
        try:
            o.path.unlink()
            result.files_deleted += 1
            result.bytes_freed += o.size
        except FileNotFoundError:
            pass
    return result


def get_snapshot_projects() -> list[tuple[str, int]]:
    """Return (dirname, size_bytes) for each snapshot project directory."""
    snap_dir = get_storage_dir() / "snapshot"
    if not snap_dir.is_dir():
        return []
    projects = []
    for d in sorted(snap_dir.iterdir()):
        if d.is_dir():
            projects.append((d.name, _dir_size(d)))
    return projects


def purge_snapshots(project: str | None = None) -> PurgeFilesResult:
    """Delete snapshot directories. If project given, delete only that one."""
    import shutil

    snap_dir = get_storage_dir() / "snapshot"
    result = PurgeFilesResult()
    if not snap_dir.is_dir():
        return result

    if project:
        target = snap_dir / project
        if target.is_dir():
            result.bytes_freed = _dir_size(target)
            shutil.rmtree(target)
            result.files_deleted = 1
    else:
        for d in snap_dir.iterdir():
            if d.is_dir():
                result.bytes_freed += _dir_size(d)
                shutil.rmtree(d)
                result.files_deleted += 1

    return result


def vacuum_db() -> tuple[int, int]:
    """Run VACUUM. Returns (before_total, after_total) including WAL+SHM."""
    path = get_db_path()
    before = _total_db_size(path)
    conn = connect(readonly=False)
    conn.execute("VACUUM")
    conn.close()
    after = _total_db_size(path)
    return before, after
