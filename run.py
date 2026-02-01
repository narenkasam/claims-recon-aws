print("### RUN.PY EXECUTED ###", flush=True)

import argparse
import csv
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import boto3
import psycopg
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client("s3")


# -----------------------------
# S3 helpers
# -----------------------------
def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.lower().startswith("s3://"):
        raise ValueError(f"Not an s3 uri: {uri}")
    u = urlparse(uri)
    bucket = u.netloc
    key = u.path.lstrip("/")
    if not bucket or key is None:
        raise ValueError(f"Invalid s3 uri: {uri}")
    return bucket, key


def ensure_prefix_uri(uri: str) -> str:
    return uri if uri.endswith("/") else uri + "/"


def s3_download_file(s3_uri: str, local_path: Path) -> None:
    bucket, key = parse_s3_uri(s3_uri)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, str(local_path))


def s3_upload_dir(local_dir: Path, s3_prefix_uri: str) -> None:
    s3_prefix_uri = ensure_prefix_uri(s3_prefix_uri)
    bucket, prefix = parse_s3_uri(s3_prefix_uri)
    for p in local_dir.rglob("*"):
        if p.is_file():
            rel = p.relative_to(local_dir).as_posix()
            key = f"{prefix.rstrip('/')}/{rel}"
            s3.upload_file(str(p), bucket, key)


def s3_list_csvs(prefix_uri: str) -> list[str]:
    """Return full s3://bucket/key URIs for CSV objects directly under a prefix."""
    prefix_uri = ensure_prefix_uri(prefix_uri)
    bucket, prefix = parse_s3_uri(prefix_uri)

    out = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/") or not key.lower().endswith(".csv"):
                continue
            out.append(f"s3://{bucket}/{key}")
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return out


def s3_move_object(src_s3_uri: str, dst_prefix_uri: str) -> str:
    """Move an object to dst prefix. Returns new s3 uri."""
    dst_prefix_uri = ensure_prefix_uri(dst_prefix_uri)
    src_bucket, src_key = parse_s3_uri(src_s3_uri)
    dst_bucket, dst_prefix = parse_s3_uri(dst_prefix_uri)

    filename = Path(src_key).name
    dst_key = f"{dst_prefix.rstrip('/')}/{filename}"

    s3.copy_object(
        Bucket=dst_bucket,
        CopySource={"Bucket": src_bucket, "Key": src_key},
        Key=dst_key,
    )
    s3.delete_object(Bucket=src_bucket, Key=src_key)
    return f"s3://{dst_bucket}/{dst_key}"


# -----------------------------
# CSV helpers
# -----------------------------
def read_header(csv_path: Path) -> list[str]:
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        header = f.readline().strip("\n").strip("\r")
    cols = [c.strip().strip('"') for c in header.split(",")]
    cols = [c for c in cols if c]
    return cols


# -----------------------------
# Postgres helpers
# -----------------------------
def pg_conn():
    host = os.getenv("PGHOST")
    port_raw = os.getenv("PGPORT", "5432")

    try:
        port = int(port_raw)
    except ValueError:
        raise RuntimeError(f"PGPORT must be numeric. Got: {port_raw!r}")

    return psycopg.connect(
        host=host,
        port=port,
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )


def exec_sql(conn, q: str):
    """
    Execute SQL safely:
      - prints SQL before running
      - rolls back on failure (prevents 'current transaction is aborted')
    """
    try:
        with conn.cursor() as cur:
            print("=== SQL ABOUT TO RUN ===", flush=True)
            print(q, flush=True)
            print("=== END SQL ===", flush=True)
            cur.execute(q)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def fetchall(conn, q: str):
    with conn.cursor() as cur:
        cur.execute(q)
        return cur.fetchall()


def create_schemas(conn):
    exec_sql(conn, "CREATE SCHEMA IF NOT EXISTS stage")
    exec_sql(conn, "CREATE SCHEMA IF NOT EXISTS metrics")


def drop_table(conn, schema: str, table: str):
    exec_sql(conn, f"DROP TABLE IF EXISTS {schema}.{table} CASCADE")


def create_text_table(conn, schema: str, table: str, cols: list[str]):
    cols_sql = ", ".join([f'"{c}" TEXT' for c in cols])
    exec_sql(conn, f'CREATE TABLE {schema}.{table} ({cols_sql})')


def copy_csv_stdin(conn, schema: str, table: str, csv_path: Path):
    """
    psycopg3-safe COPY:
      - uses HEADER true (so no manual skip)
      - streams bytes to the copy object
    """
    sql = f'COPY {schema}.{table} FROM STDIN WITH (FORMAT csv, HEADER true)'
    with conn.cursor() as cur, open(csv_path, "rb") as f:
        with cur.copy(sql) as copy:
            copy.write(f.read())
    conn.commit()


def write_query_to_csv(conn, query: str, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with conn.cursor() as cur:
        cur.execute(query)
        cols = [d.name for d in cur.description]
        rows = cur.fetchall()

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow(list(r))


def ensure_metrics_tables(conn):
    exec_sql(
        conn,
        """
        CREATE TABLE IF NOT EXISTS metrics.pipeline_run_log (
          run_ts timestamptz NOT NULL DEFAULT now(),
          run_date date NOT NULL,
          dataset text NOT NULL,
          status text NOT NULL,          -- NO_FILES | INVALID_FILE_COUNT | PROCESSED | FAILED
          inbound_prefix text NOT NULL,
          processed_prefix text NOT NULL,
          out_prefix text NOT NULL,
          old_s3 text,
          new_s3 text,
          processed_old_s3 text,
          processed_new_s3 text,
          message text,
          created_ts timestamptz NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS ix_pipeline_run_log_date
          ON metrics.pipeline_run_log (run_date, dataset);

        CREATE TABLE IF NOT EXISTS metrics.recon_summary (
          run_date date NOT NULL,
          dataset text NOT NULL,
          old_rows bigint,
          new_rows bigint,
          matched_keys bigint,
          missing_in_new bigint,
          extra_in_new bigint,
          mismatched_rows bigint,
          created_ts timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (run_date, dataset)
        );

        CREATE TABLE IF NOT EXISTS metrics.mismatch_by_column (
          run_date date NOT NULL,
          dataset text NOT NULL,
          column_name text NOT NULL,
          mismatch_rows bigint NOT NULL,
          created_ts timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (run_date, dataset, column_name)
        );

        CREATE TABLE IF NOT EXISTS metrics.mismatch_detail_sample (
          run_date date NOT NULL,
          dataset text NOT NULL,
          key_text text NOT NULL,
          record_status text NOT NULL, -- MATCHED | MISSING_IN_NEW | EXTRA_IN_NEW
          payload jsonb NOT NULL,
          created_ts timestamptz NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS ix_mismatch_detail_sample_run
          ON metrics.mismatch_detail_sample (run_date, dataset);
        """,
    )


def log_run(
    conn,
    run_date: str,
    dataset: str,
    status: str,
    inbound_prefix: str,
    processed_prefix: str,
    out_prefix: str,
    old_s3: str | None = None,
    new_s3: str | None = None,
    processed_old_s3: str | None = None,
    processed_new_s3: str | None = None,
    message: str | None = None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metrics.pipeline_run_log
              (run_date, dataset, status, inbound_prefix, processed_prefix, out_prefix,
               old_s3, new_s3, processed_old_s3, processed_new_s3, message)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                run_date,
                dataset,
                status,
                inbound_prefix,
                processed_prefix,
                out_prefix,
                old_s3,
                new_s3,
                processed_old_s3,
                processed_new_s3,
                message,
            ),
        )
    conn.commit()


def q_ident(col: str) -> str:
    return '"' + col.replace('"', '""') + '"'


# -----------------------------
# Core reconciliation SQL
# -----------------------------
def build_metrics_and_details(
    conn,
    run_date: str,
    dataset: str,
    old_table: str,
    new_table: str,
    key_cols: list[str],
    compare_cols: list[str],
    detail_limit: int,
    out_dir: Path,
):
    key_expr = " || '|' || ".join([f"COALESCE({q_ident(c)},'')" for c in key_cols])

    exec_sql(
        conn,
        f"""
        CREATE OR REPLACE VIEW stage.v_old AS
          SELECT *, ({key_expr}) AS key_text
          FROM stage.{old_table};

        CREATE OR REPLACE VIEW stage.v_new AS
          SELECT *, ({key_expr}) AS key_text
          FROM stage.{new_table}
        """,
    )

    mism_or = (
        " OR ".join([f"COALESCE(o.{q_ident(c)},'') <> COALESCE(n.{q_ident(c)},'')" for c in compare_cols])
        or "FALSE"
    )

    # FIX 1: Correctly select c (not count(*) of the CTE row)
    summary_q = f"""
    WITH
      o AS (SELECT count(*) AS c FROM stage.v_old),
      n AS (SELECT count(*) AS c FROM stage.v_new),
      matched AS (
        SELECT count(*) AS c
        FROM stage.v_old o
        JOIN stage.v_new n USING (key_text)
      ),
      miss AS (
        SELECT count(*) AS c
        FROM stage.v_old o
        LEFT JOIN stage.v_new n USING (key_text)
        WHERE n.key_text IS NULL
      ),
      extra AS (
        SELECT count(*) AS c
        FROM stage.v_new n
        LEFT JOIN stage.v_old o USING (key_text)
        WHERE o.key_text IS NULL
      ),
      mism AS (
        SELECT count(*) AS c
        FROM stage.v_old o
        JOIN stage.v_new n USING (key_text)
        WHERE ({mism_or})
      )
    SELECT
      (SELECT c FROM o) AS old_rows,
      (SELECT c FROM n) AS new_rows,
      (SELECT c FROM matched) AS matched_keys,
      (SELECT c FROM miss) AS missing_in_new,
      (SELECT c FROM extra) AS extra_in_new,
      (SELECT c FROM mism) AS mismatched_rows
    """

    write_query_to_csv(conn, summary_q, out_dir / "recon_summary.csv")

    exec_sql(
        conn,
        f"""
        INSERT INTO metrics.recon_summary
          (run_date, dataset, old_rows, new_rows, matched_keys, missing_in_new, extra_in_new, mismatched_rows)
        SELECT DATE '{run_date}', '{dataset}', old_rows, new_rows, matched_keys, missing_in_new, extra_in_new, mismatched_rows
        FROM ({summary_q}) s
        ON CONFLICT (run_date, dataset) DO UPDATE
        SET old_rows=EXCLUDED.old_rows,
            new_rows=EXCLUDED.new_rows,
            matched_keys=EXCLUDED.matched_keys,
            missing_in_new=EXCLUDED.missing_in_new,
            extra_in_new=EXCLUDED.extra_in_new,
            mismatched_rows=EXCLUDED.mismatched_rows,
            created_ts=now()
        """,
    )

    # mismatch by column counts (matched keys)
    col_sums = ",\n".join(
        [
            f"""COALESCE(
                SUM(CASE WHEN COALESCE(o.{q_ident(c)},'') <> COALESCE(n.{q_ident(c)},'')
                        THEN 1 ELSE 0 END),
                0
            ) AS {q_ident(c)}"""
            for c in compare_cols
        ]
    ) or "0 AS dummy"
    
    # FIX 2: remove trailing semicolon
    bycol_pivot_q = f"""
    SELECT
      {col_sums}
    FROM stage.v_old o
    JOIN stage.v_new n USING (key_text)
    """

    if compare_cols:
        row = fetchall(conn, bycol_pivot_q)[0]
        mismatch_counts = list(zip(compare_cols, [int(x) for x in row]))
    else:
        mismatch_counts = []

    with open(out_dir / "mismatch_by_column.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["column_name", "mismatch_rows"])
        for c, v in sorted(mismatch_counts, key=lambda t: t[1], reverse=True):
            w.writerow([c, v])

    exec_sql(
        conn,
        f"DELETE FROM metrics.mismatch_by_column WHERE run_date = DATE '{run_date}' AND dataset = '{dataset}'",
    )
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO metrics.mismatch_by_column (run_date, dataset, column_name, mismatch_rows)
            VALUES (%s, %s, %s, %s)
            """,
            [(run_date, dataset, c, v) for c, v in mismatch_counts],
        )
    conn.commit()

    # detail sample
    payload_parts = []
    for c in compare_cols:
        payload_parts.append(
            f"""
            CASE
              WHEN o.key_text IS NULL THEN jsonb_build_object('{c}', ('~' || COALESCE(n.{q_ident(c)},'')))
              WHEN n.key_text IS NULL THEN jsonb_build_object('{c}', (COALESCE(o.{q_ident(c)},'') || '~'))
              WHEN COALESCE(o.{q_ident(c)},'') <> COALESCE(n.{q_ident(c)},'') THEN jsonb_build_object('{c}', (COALESCE(o.{q_ident(c)},'') || '~' || COALESCE(n.{q_ident(c)},'')))
              ELSE '{{}}'::jsonb
            END
            """.strip()
        )

    payload_concat = " || ".join(payload_parts) if payload_parts else "'{}'::jsonb"

    # FIX 3: remove trailing semicolon
    detail_q = f"""
    WITH j AS (
      SELECT
        COALESCE(o.key_text, n.key_text) AS key_text,
        CASE
          WHEN o.key_text IS NULL THEN 'EXTRA_IN_NEW'
          WHEN n.key_text IS NULL THEN 'MISSING_IN_NEW'
          ELSE 'MISMATCHED'
        END AS record_status,
        ({payload_concat}) AS payload
      FROM stage.v_old o
      FULL OUTER JOIN stage.v_new n USING (key_text)
      WHERE
        o.key_text IS NULL
        OR n.key_text IS NULL
        OR ({mism_or})
    )
    SELECT key_text, record_status, payload
    FROM j
    WHERE payload <> '{{}}'::jsonb
    ORDER BY record_status, key_text
    LIMIT {int(detail_limit)}
    """

    write_query_to_csv(conn, detail_q, out_dir / f"mismatch_detail_sample_{detail_limit}.csv")

    exec_sql(
        conn,
        f"""
        DELETE FROM metrics.mismatch_detail_sample
        WHERE run_date = DATE '{run_date}' AND dataset = '{dataset}'
        """,
    )
    exec_sql(
        conn,
        f"""
        INSERT INTO metrics.mismatch_detail_sample (run_date, dataset, key_text, record_status, payload)
        SELECT DATE '{run_date}', '{dataset}', key_text, record_status, payload
        FROM ({detail_q}) x
        """,
    )


# -----------------------------
# Daily pipeline logic
# -----------------------------
def pick_old_new(files: list[str]) -> tuple[str, str]:
    """Exactly 2 files present. New file contains NEWSYSTEM in name."""
    if len(files) != 2:
        raise ValueError("pick_old_new requires exactly 2 files")

    f1, f2 = files
    n1 = "NEWSYSTEM" in Path(urlparse(f1).path).name.upper()
    n2 = "NEWSYSTEM" in Path(urlparse(f2).path).name.upper()

    if n1 and not n2:
        return f2, f1  # old, new
    if n2 and not n1:
        return f1, f2  # old, new

    raise RuntimeError("Could not identify OLD vs NEW: expected exactly one filename containing 'NEWSYSTEM'.")


def main(
    inbound_s3_prefix: str,
    processed_s3_prefix: str,
    out_s3_prefix: str,
    dataset: str,
    key_cols: list[str],
    detail_limit: int,
):
    inbound_s3_prefix = ensure_prefix_uri(inbound_s3_prefix)
    processed_s3_prefix = ensure_prefix_uri(processed_s3_prefix)
    out_s3_prefix = ensure_prefix_uri(out_s3_prefix)

    run_date = datetime.now(timezone.utc).date().isoformat()

    conn = pg_conn()
    create_schemas(conn)
    ensure_metrics_tables(conn)

    try:
        files = s3_list_csvs(inbound_s3_prefix)

        if len(files) == 0:
            log_run(
                conn=conn,
                run_date=run_date,
                dataset=dataset,
                status="NO_FILES",
                inbound_prefix=inbound_s3_prefix,
                processed_prefix=processed_s3_prefix,
                out_prefix=out_s3_prefix,
                message="No CSV files found in inbound prefix.",
            )
            print(f"[{run_date}] NO_FILES: nothing to process.", flush=True)
            return

        if len(files) != 2:
            log_run(
                conn=conn,
                run_date=run_date,
                dataset=dataset,
                status="INVALID_FILE_COUNT",
                inbound_prefix=inbound_s3_prefix,
                processed_prefix=processed_s3_prefix,
                out_prefix=out_s3_prefix,
                message=f"Expected exactly 2 CSV files in inbound prefix, found {len(files)}.",
            )
            raise RuntimeError(f"Expected exactly 2 CSVs in inbound, found {len(files)}. Clean inbound/ and retry.")

        old_s3, new_s3 = pick_old_new(files)
        print(f"[{run_date}] Found inputs:\n  OLD: {old_s3}\n  NEW: {new_s3}", flush=True)

        work = Path(tempfile.mkdtemp(prefix="claims_recon_"))
        in_dir = work / "in"
        out_dir = work / "out"
        in_dir.mkdir(parents=True, exist_ok=True)
        out_dir.mkdir(parents=True, exist_ok=True)

        old_local = in_dir / "old.csv"
        new_local = in_dir / "new.csv"

        print(f"[{run_date}] Downloading inputs...", flush=True)
        s3_download_file(old_s3, old_local)
        s3_download_file(new_s3, new_local)

        old_cols = read_header(old_local)
        new_cols = read_header(new_local)

        if [c.lower() for c in old_cols] != [c.lower() for c in new_cols]:
            raise RuntimeError("Old/New CSV headers do not match. This pipeline expects identical schema.")

        missing_keys = [k for k in key_cols if k.lower() not in [c.lower() for c in old_cols]]
        if missing_keys:
            raise RuntimeError(f"Key columns not found in CSV header: {missing_keys}")

        key_lower = {k.lower() for k in key_cols}
        compare_cols = [c for c in old_cols if c.lower() not in key_lower]

        print(f"[{run_date}] Loading stage tables...", flush=True)
        drop_table(conn, "stage", "raw_daily_old")
        drop_table(conn, "stage", "raw_daily_new")
        create_text_table(conn, "stage", "raw_daily_old", old_cols)
        create_text_table(conn, "stage", "raw_daily_new", new_cols)
        copy_csv_stdin(conn, "stage", "raw_daily_old", old_local)
        copy_csv_stdin(conn, "stage", "raw_daily_new", new_local)
        old_cnt = fetchall(conn, "SELECT count(*) FROM stage.raw_daily_old")[0][0]
        new_cnt = fetchall(conn, "SELECT count(*) FROM stage.raw_daily_new")[0][0]
        print(f"Loaded stage.raw_daily_old rows={old_cnt}", flush=True)
        print(f"Loaded stage.raw_daily_new rows={new_cnt}", flush=True)


        print(f"[{run_date}] Computing metrics + mismatch detail sample (limit={detail_limit})...", flush=True)
        build_metrics_and_details(
            conn=conn,
            run_date=run_date,
            dataset=dataset,
            old_table="raw_daily_old",
            new_table="raw_daily_new",
            key_cols=key_cols,
            compare_cols=compare_cols,
            detail_limit=detail_limit,
            out_dir=out_dir,
        )

        print(f"[{run_date}] Uploading outputs to {out_s3_prefix} ...", flush=True)
        s3_upload_dir(out_dir, out_s3_prefix)

        print(f"[{run_date}] Moving processed inputs to {processed_s3_prefix} ...", flush=True)
        processed_old = s3_move_object(old_s3, processed_s3_prefix)
        processed_new = s3_move_object(new_s3, processed_s3_prefix)

        log_run(
            conn=conn,
            run_date=run_date,
            dataset=dataset,
            status="PROCESSED",
            inbound_prefix=inbound_s3_prefix,
            processed_prefix=processed_s3_prefix,
            out_prefix=out_s3_prefix,
            old_s3=old_s3,
            new_s3=new_s3,
            processed_old_s3=processed_old,
            processed_new_s3=processed_new,
            message=f"Processed 2 files. Wrote outputs + metrics. Detail sample limit={detail_limit}.",
        )

        shutil.rmtree(work, ignore_errors=True)
        print("DONE.", flush=True)

    except Exception as e:
        # IMPORTANT: rollback so we can still log_run safely
        try:
            conn.rollback()
        except Exception:
            pass

        try:
            log_run(
                conn=conn,
                run_date=run_date,
                dataset=dataset,
                status="FAILED",
                inbound_prefix=inbound_s3_prefix,
                processed_prefix=processed_s3_prefix,
                out_prefix=out_s3_prefix,
                message=str(e),
            )
        except Exception as log_e:
            print(f"FAILED to log_run due to: {log_e}", flush=True)

        raise
    finally:
        conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--inbound-s3", required=True, help="s3://.../claims/inbound/")
    ap.add_argument("--processed-s3", required=True, help="s3://.../claims/processed/")
    ap.add_argument("--out-s3", required=True, help="s3://.../claims/out/")
    ap.add_argument("--dataset", default="daily", help="label used in metrics tables")
    ap.add_argument("--keys", required=True, help="comma-separated key columns, e.g. DESYNPUF_ID,CLM_ID")
    ap.add_argument("--detail-limit", type=int, default=5000)
    args = ap.parse_args()

    keys = [k.strip() for k in args.keys.split(",") if k.strip()]

    main(
        inbound_s3_prefix=args.inbound_s3,
        processed_s3_prefix=args.processed_s3,
        out_s3_prefix=args.out_s3,
        dataset=args.dataset,
        key_cols=keys,
        detail_limit=args.detail_limit,
    )
