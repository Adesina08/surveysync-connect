def _ensure_schema(cur, schema: str) -> None:
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))


def _table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        )
        """,
        (schema, table),
    )
    return bool(cur.fetchone()[0])


def _get_existing_columns(cur, schema: str, table: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return {r[0] for r in cur.fetchall()}


def _create_table(cur, schema: str, table: str, cols: list[str], pk: str | None) -> None:
    # Default everything to TEXT. SurveyCTO values are typically strings.
    col_defs = [sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in cols]

    if pk and pk in cols:
        # Make pk NOT NULL + PRIMARY KEY
        col_defs = [
            sql.SQL("{} TEXT PRIMARY KEY").format(sql.Identifier(pk))
            if c == pk
            else sql.SQL("{} TEXT").format(sql.Identifier(c))
            for c in cols
        ]

    cur.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(col_defs) if col_defs else sql.SQL("dummy TEXT"),
        )
    )


def _add_missing_columns(cur, schema: str, table: str, desired_cols: list[str]) -> None:
    existing = _get_existing_columns(cur, schema, table)
    missing = [c for c in desired_cols if c not in existing]
    for c in missing:
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} TEXT").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(c),
            )
        )


def _ensure_table_ready(cur, schema: str, table: str, cols: list[str], sync_mode: str, pk: str) -> None:
    _ensure_schema(cur, schema)

    # For upsert, we need the PK column to exist.
    desired = list(cols)
    if sync_mode == "upsert" and pk not in desired:
        desired.append(pk)

    if not _table_exists(cur, schema, table):
        _create_table(cur, schema, table, desired, pk if sync_mode == "upsert" else None)
    else:
        _add_missing_columns(cur, schema, table, desired)
