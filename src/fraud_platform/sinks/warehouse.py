"""Idempotent warehouse writers.

Spark ``foreachBatch`` is at-least-once: after a crash the same ``batch_id`` is replayed.
Every write here is therefore idempotent:

* keyed tables  -> INSERT ... ON CONFLICT (keys) DO NOTHING / DO UPDATE  (Snowflake: MERGE)
* append tables -> DELETE WHERE pipeline=? AND batch_id=?  then INSERT (same transaction)

Implementations: DuckDB (local, zero-setup), PostgreSQL (docker-compose stand-in for
Snowflake) and Snowflake (production).
"""
from __future__ import annotations

import logging
import os
import threading
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from .tables import TABLES

log = logging.getLogger(__name__)
SQL_DIR = Path(os.environ.get("FRAUD_SQL_DIR", Path(__file__).resolve().parents[3] / "sql"))


def _prepare(table: str, df: pd.DataFrame) -> pd.DataFrame:
    cols = TABLES[table]["columns"]
    out = df.reindex(columns=cols)
    # warehouses expect None, not NaN/NaT
    return out.astype(object).where(pd.notna(out), None)


class WarehouseSink(ABC):
    @abstractmethod
    def ensure_schema(self) -> None: ...

    @abstractmethod
    def upsert(self, table: str, df: pd.DataFrame, update: bool = False) -> int: ...

    @abstractmethod
    def replace_batch(self, table: str, df: pd.DataFrame, pipeline: str, batch_id: int) -> int: ...

    @abstractmethod
    def query(self, sql: str, params: tuple | list | None = None) -> pd.DataFrame: ...

    def close(self) -> None:  # noqa: B027 - optional hook, default no-op
        pass


# --------------------------------------------------------------------------- SQL helpers
def _insert_sql(table: str, update: bool, placeholder: str = "?") -> str:
    meta = TABLES[table]
    cols, keys = meta["columns"], meta["keys"]
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join([placeholder] * len(cols))})"
    if keys:
        non_keys = [c for c in cols if c not in keys]
        if update and non_keys:
            sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in non_keys)
            sql += f" ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {sets}"
        else:
            sql += f" ON CONFLICT ({', '.join(keys)}) DO NOTHING"
    return sql


def _split_sql(script: str) -> list[str]:
    stmts, buf = [], []
    for line in script.splitlines():
        if line.strip().startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            stmts.append("\n".join(buf).strip().rstrip(";"))
            buf = []
    if "".join(buf).strip():
        stmts.append("\n".join(buf).strip())
    return [s for s in stmts if s]


# --------------------------------------------------------------------------- DuckDB
class DuckDBSink(WarehouseSink):
    """Local file warehouse. Opens a short-lived connection per call so readers
    (investigation console, tests) can open the file between micro-batches."""

    _lock = threading.Lock()

    def __init__(self, path: str):
        import duckdb  # noqa: F401  (fail fast if missing)

        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    def _connect(self, read_only: bool = False):
        import duckdb

        return duckdb.connect(self.path, read_only=read_only)

    def ensure_schema(self) -> None:
        with self._lock, self._connect() as con:
            for f in ("01_schema.sql", "02_marts.sql"):
                for stmt in _split_sql((SQL_DIR / "postgres" / f).read_text()):
                    con.execute(stmt)

    @staticmethod
    def _insert_from_frame(con, table: str, df: pd.DataFrame, update: bool) -> None:
        """Vectorised INSERT ... SELECT from the registered DataFrame (much faster than executemany)."""
        cols = TABLES[table]["columns"]
        frame = df.reindex(columns=cols)
        for c in frame.columns:                  # all-null object columns -> explicit NULLs DuckDB can cast
            if frame[c].dtype == object and frame[c].isna().all():
                frame[c] = pd.Series([None] * len(frame), dtype="string")
        con.register("_incoming", frame)
        try:
            sql = _insert_sql(table, update)
            head, _, tail = sql.partition(" VALUES (")
            conflict = tail.split(")", 1)[1] if ")" in tail else ""
            con.execute(f"{head} SELECT {', '.join(cols)} FROM _incoming{conflict}")
        finally:
            con.unregister("_incoming")

    def upsert(self, table: str, df: pd.DataFrame, update: bool = False) -> int:
        if df.empty:
            return 0
        keys = TABLES[table]["keys"]
        if keys:   # a frame may contain the same key twice -> keep first (ON CONFLICT can't see in-batch dups)
            df = df.drop_duplicates(subset=keys, keep="first")
        with self._lock, self._connect() as con:
            self._insert_from_frame(con, table, df, update)
        return len(df)

    def replace_batch(self, table: str, df: pd.DataFrame, pipeline: str, batch_id: int) -> int:
        with self._lock, self._connect() as con:
            con.execute("BEGIN")
            con.execute(f"DELETE FROM {table} WHERE pipeline = ? AND batch_id = ?", [pipeline, batch_id])
            if not df.empty:
                self._insert_from_frame(con, table, df, False)
            con.execute("COMMIT")
        return len(df)

    def query(self, sql: str, params=None) -> pd.DataFrame:
        with self._lock, self._connect() as con:
            return con.execute(sql, params or []).fetchdf()


# --------------------------------------------------------------------------- PostgreSQL
class PostgresSink(WarehouseSink):
    def __init__(self, dsn: str):
        import psycopg2  # noqa: F401

        self.dsn = dsn
        self._lock = threading.Lock()

    def _connect(self):
        import psycopg2

        return psycopg2.connect(self.dsn)

    def ensure_schema(self) -> None:
        with self._lock:
            con = self._connect()
            try:
                with con, con.cursor() as cur:
                    for f in ("01_schema.sql", "02_marts.sql"):
                        cur.execute((SQL_DIR / "postgres" / f).read_text())
            finally:
                con.close()

    def _write(self, table: str, df: pd.DataFrame, update: bool, delete: tuple | None) -> int:
        from psycopg2.extras import execute_values

        meta = TABLES[table]
        rows = [tuple(r) for r in _prepare(table, df).values.tolist()] if not df.empty else []
        sql = _insert_sql(table, update).replace(
            f"VALUES ({', '.join(['?'] * len(meta['columns']))})", "VALUES %s")
        with self._lock:
            con = self._connect()
            try:
                with con, con.cursor() as cur:       # single transaction
                    if delete:
                        cur.execute(f"DELETE FROM {table} WHERE pipeline = %s AND batch_id = %s", delete)
                    if rows:
                        execute_values(cur, sql, rows, page_size=1000)
            finally:
                con.close()
        return len(rows)

    def upsert(self, table: str, df: pd.DataFrame, update: bool = False) -> int:
        return 0 if df.empty else self._write(table, df, update, None)

    def replace_batch(self, table: str, df: pd.DataFrame, pipeline: str, batch_id: int) -> int:
        return self._write(table, df, False, (pipeline, int(batch_id)))

    def query(self, sql: str, params=None) -> pd.DataFrame:
        con = self._connect()
        try:
            with con.cursor() as cur:
                cur.execute(sql.replace("?", "%s"), params or None)
                cols = [d[0] for d in cur.description]
                return pd.DataFrame(cur.fetchall(), columns=cols)
        finally:
            con.close()


# --------------------------------------------------------------------------- Snowflake
class SnowflakeSink(WarehouseSink):
    """Production sink: write_pandas into a temporary stage table, then MERGE.

    Authentication uses key-pair auth; the private key is read from a file path that is
    populated from AWS Secrets Manager (see infra/terraform/secrets.tf).
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self._lock = threading.Lock()
        self._con = None

    def _connect(self):
        if self._con is None or self._con.is_closed():
            import snowflake.connector
            from cryptography.hazmat.primitives import serialization

            with open(self.cfg["private_key_path"], "rb") as fh:
                pkey = serialization.load_pem_private_key(fh.read(), password=None)
            pkb = pkey.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
                                     serialization.NoEncryption())
            self._con = snowflake.connector.connect(
                account=self.cfg["account"], user=self.cfg["user"], private_key=pkb,
                role=self.cfg.get("role"), warehouse=self.cfg.get("warehouse"),
                database=self.cfg.get("database"), session_parameters={"TIMEZONE": "UTC"},
            )
        return self._con

    def ensure_schema(self) -> None:
        log.info("Snowflake schema is managed by sql/snowflake/*.sql (deployed via CI), skipping")

    def _stage(self, con, table: str, df: pd.DataFrame) -> str:
        from snowflake.connector.pandas_tools import write_pandas

        stage = f"{table.split('.')[0]}.STG_{table.split('.')[1]}".upper()
        con.cursor().execute(f"CREATE TEMPORARY TABLE IF NOT EXISTS {stage} LIKE {table.upper()}")
        con.cursor().execute(f"TRUNCATE TABLE {stage}")
        pdf = _prepare(table, df)
        pdf.columns = [c.upper() for c in pdf.columns]
        schema, name = stage.split(".")
        write_pandas(con, pdf, name, schema=schema, auto_create_table=False, use_logical_type=True)
        return stage

    def upsert(self, table: str, df: pd.DataFrame, update: bool = False) -> int:
        if df.empty:
            return 0
        meta = TABLES[table]
        cols, keys = meta["columns"], meta["keys"]
        with self._lock:
            con = self._connect()
            stage = self._stage(con, table, df)
            on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
            sql = f"MERGE INTO {table} t USING (SELECT * FROM {stage} QUALIFY ROW_NUMBER() OVER " \
                  f"(PARTITION BY {', '.join(keys)} ORDER BY 1) = 1) s ON {on} "
            if update:
                sets = ", ".join(f"t.{c} = s.{c}" for c in cols if c not in keys)
                sql += f"WHEN MATCHED THEN UPDATE SET {sets} "
            sql += f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})"
            con.cursor().execute(sql)
        return len(df)

    def replace_batch(self, table: str, df: pd.DataFrame, pipeline: str, batch_id: int) -> int:
        with self._lock:
            con = self._connect()
            stage = self._stage(con, table, df) if not df.empty else None   # DDL: outside the txn
            cur = con.cursor()
            cur.execute("BEGIN")
            cur.execute(f"DELETE FROM {table} WHERE pipeline = %s AND batch_id = %s", (pipeline, int(batch_id)))
            if stage:
                cols = ", ".join(TABLES[table]["columns"])
                cur.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM {stage}")
            cur.execute("COMMIT")
        return len(df)

    def query(self, sql: str, params=None) -> pd.DataFrame:
        cur = self._connect().cursor()
        cur.execute(sql.replace("?", "%s"), params)
        return cur.fetch_pandas_all()

    def close(self) -> None:
        if self._con is not None:
            self._con.close()


def build_sink(settings: dict) -> WarehouseSink:
    wcfg = settings.get("warehouse", {})
    kind = wcfg.get("type", "duckdb")
    if kind == "duckdb":
        return DuckDBSink(wcfg.get("duckdb_path", "data/warehouse/fraud.duckdb"))
    if kind == "postgres":
        return PostgresSink(wcfg["postgres_dsn"])
    if kind == "snowflake":
        return SnowflakeSink(wcfg["snowflake"])
    raise ValueError(f"unknown warehouse type: {kind}")
