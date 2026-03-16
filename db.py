"""
Neon database helper for storing latest oil prices.
"""
from __future__ import annotations

import os
from datetime import date

import psycopg2
from psycopg2.extras import execute_batch


NEON_DATABASE_URL = os.environ.get("NEON_DATABASE_URL")


def get_conn():
    if not NEON_DATABASE_URL:
        raise RuntimeError("NEON_DATABASE_URL environment variable is not set")
    return psycopg2.connect(NEON_DATABASE_URL)


def _ensure_schema(cur) -> None:
    """
    Create minimal dim_oil_types and fact_prices tables if they don't exist.
    This is intentionally small and focused on yfinance price storage only.
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_oil_types (
            id          SERIAL PRIMARY KEY,
            code        TEXT UNIQUE NOT NULL,
            name        TEXT NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_prices (
            id              BIGSERIAL PRIMARY KEY,
            oil_type_id     INT NOT NULL REFERENCES dim_oil_types(id),
            price_date      DATE NOT NULL,
            close_price     NUMERIC(18, 6) NOT NULL,
            change_percent  NUMERIC(10, 4),
            src             TEXT NOT NULL,
            created_at      TIMESTAMPTZ DEFAULT now(),
            UNIQUE (oil_type_id, price_date, src)
        );
        """
    )


def upsert_prices(df) -> None:
    """
    Store latest prices into Neon.

    Expects a DataFrame with columns:
      - Name
      - Symbol
      - Price
      - Change %
    """
    if df is None or df.empty:
        return

    rows = []
    for _, row in df.iterrows():
        rows.append(
            (
                row.get("Symbol"),
                row.get("Name") or row.get("Symbol"),
                row.get("Price"),
                row.get("Change %"),
            )
        )

    with get_conn() as conn:
        with conn.cursor() as cur:
            _ensure_schema(cur)

            # Load existing oil types
            cur.execute("SELECT code, id FROM dim_oil_types")
            existing = {code: oid for code, oid in cur.fetchall()}

            # Insert any new oil types we see
            for code, name, _price, _chg in rows:
                if not code or code in existing:
                    continue
                cur.execute(
                    """
                    INSERT INTO dim_oil_types (code, name)
                    VALUES (%s, %s)
                    ON CONFLICT (code) DO NOTHING
                    RETURNING id;
                    """,
                    (code, name or code),
                )
                res = cur.fetchone()
                if res:
                    existing[code] = res[0]

            today = date.today()
            data_for_insert = []
            for code, _name, price, change_pct in rows:
                oil_type_id = existing.get(code)
                if oil_type_id is None or price is None:
                    continue
                data_for_insert.append(
                    (oil_type_id, today, price, change_pct, "yfinance")
                )

            if not data_for_insert:
                return

            execute_batch(
                cur,
                """
                INSERT INTO fact_prices (
                    oil_type_id,
                    price_date,
                    close_price,
                    change_percent,
                    src
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (oil_type_id, price_date, src) DO NOTHING;
                """,
                data_for_insert,
            )

