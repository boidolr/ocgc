"""Click CLI entry point."""

import click


@click.group()
@click.version_option(package_name="ocgc", prog_name="ocgc")
def cli() -> None:
    """ocgc - OpenCode Garbage Collector.

    Analyze and reclaim storage used by OpenCode sessions, diffs, and snapshots.
    """


@cli.command()
def status() -> None:
    """Dashboard: DB size, session count, storage breakdown."""
    from ocgc.analyzer import run_status

    run_status()


@cli.command()
@click.option(
    "-s", "--sort", "sort_by", type=click.Choice(["size", "age", "name"]), default="size", help="Sort sessions by"
)
@click.option("--limit", "-l", type=int, default=None, help="Limit number of sessions shown")
def sessions(sort_by: str, limit: int | None) -> None:
    """List sessions with sizes, ages, and types."""
    from ocgc.analyzer import run_sessions

    run_sessions(sort_by=sort_by, limit=limit)


@cli.command()
def analyze() -> None:
    """Deep analysis: biggest sessions, part type breakdown, growth rate."""
    from ocgc.analyzer import run_analyze

    run_analyze()


@cli.command()
@click.option(
    "-o", "--older-than", default=None, help="Delete sessions older than duration (e.g., 15m, 1h, 7d, 2w, 3mo)"
)
@click.option("-s", "--subagents", is_flag=True, default=False, help="Delete subagent sessions (parent_id IS NOT NULL)")
@click.option("-b", "--larger-than", default=None, help="Delete sessions larger than size (e.g., 50M, 1G)")
@click.option(
    "-r", "--strip-reasoning", is_flag=True, default=False, help="Remove reasoning parts only (keeps sessions)"
)
@click.option("-S", "--session", "session_ids", multiple=True, help="Delete specific session by ID (repeatable)")
@click.option("-k", "--keep-latest", type=int, default=None, help="Keep N most recent sessions, delete the rest")
@click.option("-p", "--clean-snapshots", is_flag=True, default=False, help="Delete all snapshot directories")
@click.option(
    "-O",
    "--clean-orphans",
    is_flag=True,
    default=False,
    help="Delete orphan session diff files (no matching session)",
)
@click.option(
    "-m",
    "--clean-migrated",
    is_flag=True,
    default=False,
    help="Delete stale v1 copies of sessions migrated to v2; v1-only messages are lost permanently",
)
@click.option("-l", "--clean-logs", is_flag=True, default=False, help="Delete files in the opencode log directory")
@click.option("--dry-run", "-n", is_flag=True, default=False, help="Show what would be deleted without doing it")
@click.option("--force", "-f", is_flag=True, default=False, help="Skip confirmation prompt")
def purge(
    older_than: str | None,
    subagents: bool,
    larger_than: str | None,
    strip_reasoning: bool,
    session_ids: tuple[str, ...],
    keep_latest: int | None,
    clean_snapshots: bool,
    clean_orphans: bool,
    clean_migrated: bool,
    clean_logs: bool,
    dry_run: bool,
    force: bool,
) -> None:
    """Delete sessions by age, type, size, or ID."""
    if keep_latest is not None and keep_latest < 0:
        raise click.BadParameter("must be a non-negative integer", param_hint="'--keep-latest'")
    from ocgc.purger import run_clean_logs, run_clean_migrated, run_clean_orphans, run_clean_snapshots, run_purge

    session_flags = (
        bool(older_than),
        bool(subagents),
        bool(larger_than),
        bool(session_ids),
        keep_latest is not None,
        strip_reasoning,
    )
    clean_flags = (clean_snapshots, clean_orphans, clean_migrated, clean_logs)

    if clean_snapshots:
        run_clean_snapshots(dry_run=dry_run, force=force)

    if clean_orphans:
        run_clean_orphans(dry_run=dry_run, force=force)

    if clean_migrated:
        run_clean_migrated(dry_run=dry_run, force=force)

    if clean_logs:
        run_clean_logs(dry_run=dry_run, force=force)

    # With clean-only flags there is nothing more to do; with no flags at all
    # run_purge still reports the missing-flag error.
    if any(clean_flags) and not any(session_flags):
        return

    run_purge(
        older_than=older_than,
        subagents=subagents,
        larger_than=larger_than,
        strip_reasoning=strip_reasoning,
        session_ids=session_ids,
        keep_latest=keep_latest,
        dry_run=dry_run,
        force=force,
    )


@cli.command()
@click.option("--force", "-f", is_flag=True, default=False, help="Skip confirmation prompt")
def vacuum(force: bool) -> None:
    """Run VACUUM to reclaim disk space after purge."""
    from ocgc.purger import run_vacuum

    run_vacuum(force=force)
