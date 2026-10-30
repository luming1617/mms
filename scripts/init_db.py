from __future__ import annotations

import os
from pathlib import Path

import psycopg
from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / "schema" / "schema.sql"

UPGRADE_BEGIN = "-- >>> mms:upgrade"
UPGRADE_END = "-- <<< mms:upgrade"

# Docs 12.3: schema.sql declares the runtime roles NOLOGIN; environment bootstrap turns
# them into logins. Every password must be supplied, so a half-provisioned cluster fails
# loudly instead of silently leaving a role unusable.
LOGIN_ROLES = (
    ("mms_app", "MMS_APP_DB_PASSWORD"),
    ("mms_worker", "MMS_WORKER_DB_PASSWORD"),
    ("mms_readonly", "MMS_READONLY_DB_PASSWORD"),
    ("mms_audit_scrubber", "MMS_SCRUBBER_DB_PASSWORD"),
    ("mms_deletion_worker", "MMS_DELETION_DB_PASSWORD"),
)


def upgrade_delta(sql_text: str) -> str:
    """Docs 12.1/12.3: additive, idempotent blocks an existing database replays to catch up."""
    parts = []
    for chunk in sql_text.split(UPGRADE_BEGIN)[1:]:
        body, found, _ = chunk.partition(UPGRADE_END)
        if not found:
            raise SystemExit(f"unterminated {UPGRADE_BEGIN} region in {SCHEMA}")
        parts.append(body)
    return "\n".join(parts)


def provision_login_roles(conn) -> None:
    for role, env_key in LOGIN_ROLES:
        password = os.environ.get(env_key)
        if not password:
            raise SystemExit(f"{env_key} is required to enable {role} as a login role")
        if conn.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,)).fetchone() is None:
            conn.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(role)))
        # ALTER ROLE is a utility statement: it takes no parameters, so the literal is
        # composed with psycopg's quoting helper instead of a bound value.
        conn.execute(sql.SQL("ALTER ROLE {} LOGIN PASSWORD {}").format(sql.Identifier(role), sql.Literal(password)))


def main() -> None:
    dsn = os.environ.get("MMS_DATABASE_URL", "postgresql://mms:mms@127.0.0.1:5433/mms")
    sql_text = SCHEMA.read_text(encoding="utf-8")
    with psycopg.connect(dsn) as conn, conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended('mms_schema_init', 0))")
        state = conn.execute(
            """
            SELECT to_regclass('public.customer_memory') IS NOT NULL AS started,
                   (to_regclass('public.customer_memory') IS NOT NULL
                    AND to_regclass('public.memory_translation_job') IS NOT NULL
                    AND EXISTS (SELECT 1 FROM information_schema.columns
                                WHERE table_schema='public' AND table_name='memory_usage_event'
                                  AND column_name='matched_views')) AS complete
            """
        ).fetchone()
        if not state[1]:
            if state[0]:
                raise SystemExit("partial schema detected; recreate the database before initialization")
            conn.execute(sql_text)
            print("schema initialized")
        else:
            conn.execute(upgrade_delta(sql_text))
            print("schema already initialized; upgrade delta applied")
        if os.environ.get("MMS_PROVISION_LOGIN_ROLES") == "1":
            provision_login_roles(conn)
            print("runtime login roles provisioned")


if __name__ == "__main__":
    main()
