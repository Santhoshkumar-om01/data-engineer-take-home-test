"""
Application Log Ingestion Service to MySQL (Step 2).

Parses raw application log files and ingests normalized records
into a target MySQL database application table in batches.
"""

import argparse
import ast
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ApplicationRecord:
    id: str
    therapeutic_area: str
    created_at: str
    site_name: str
    site_category: str

    def to_row(self) -> Tuple[str, str, str, str, str]:
        return (
            self.id,
            self.therapeutic_area,
            self.created_at,
            self.site_name,
            self.site_category,
        )


class ApplicationLogParser:
    """Parses delimited log strings into ApplicationRecord instances."""

    DELIMITER = "|"
    KV_SEPARATOR = "="

    @classmethod
    def parse_file(cls, file_path: Path) -> Optional[ApplicationRecord]:
        try:
            content = file_path.read_text(encoding="utf-8").strip()
            if not content:
                return None

            fields: Dict[str, str] = {}
            for token in content.split(cls.DELIMITER):
                token = token.strip()
                if cls.KV_SEPARATOR in token:
                    k, v = token.split(cls.KV_SEPARATOR, 1)
                    fields[k.strip()] = v.strip()

            app_id = fields.get("id")
            therapeutic_area = fields.get("therapeutic_area")
            created_at = fields.get("created_at")
            site_raw = fields.get("site")

            if not all([app_id, therapeutic_area, created_at, site_raw]):
                return None

            site_dict = ast.literal_eval(site_raw)
            site_name = site_dict.get("site_name") or site_dict.get("name")
            site_category = site_dict.get("site_category") or site_dict.get("category")

            return ApplicationRecord(
                id=str(app_id),
                therapeutic_area=str(therapeutic_area),
                created_at=str(created_at),
                site_name=str(site_name),
                site_category=str(site_category),
            )
        except Exception as exc:
            logger.warning("Failed parsing %s: %s", file_path.name, exc)
            return None


class MySQLApplicationLoader:
    """Handles schema management and batch ingestion into MySQL."""

    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS application (
        id VARCHAR(100),
        therapeutic_area VARCHAR(10),
        created_at TIMESTAMP,
        site_name VARCHAR(50),
        site_category VARCHAR(20)
    );
    """

    INSERT_SQL = """
    INSERT INTO application (id, therapeutic_area, created_at, site_name, site_category)
    VALUES (%s, %s, %s, %s, %s);
    """

    def __init__(self, db_config: Dict[str, Any]) -> None:
        self.db_config = db_config

    def _get_connection(self):
        """Attempts to establish connection via mysql.connector or pymysql."""
        try:
            import mysql.connector

            return mysql.connector.connect(**self.db_config)
        except ImportError:
            try:
                import pymysql

                return pymysql.connect(**self.db_config)
            except ImportError as err:
                raise ImportError(
                    "Neither 'mysql-connector-python' nor 'pymysql' is installed."
                ) from err

    def init_schema(self, connection) -> None:
        """Initializes application table schema if not already present."""
        with connection.cursor() as cursor:
            cursor.execute(self.CREATE_TABLE_SQL)
        connection.commit()
        logger.info("Database schema initialized successfully.")

    def insert_batch(self, connection, records: List[ApplicationRecord]) -> int:
        """Inserts records in batch using parameterized query."""
        if not records:
            return 0
        rows = [rec.to_row() for rec in records]
        with connection.cursor() as cursor:
            cursor.executemany(self.INSERT_SQL, rows)
        connection.commit()
        return len(rows)

    def run_pipeline(self, source_dir: Path, batch_size: int = 500) -> None:
        """Executes log ingestion in configurable batches."""
        log_files = list(source_dir.glob("*.json"))
        logger.info("Found %d application logs to ingest.", len(log_files))

        try:
            conn = self._get_connection()
        except Exception as exc:
            logger.error("Could not connect to MySQL database: %s", exc)
            logger.info("Dry-run validation: validating parsing logic across all files.")
            valid_count = sum(1 for f in log_files if ApplicationLogParser.parse_file(f))
            logger.info("Validated %d/%d records successfully without DB.", valid_count, len(log_files))
            return

        try:
            self.init_schema(conn)
            batch: List[ApplicationRecord] = []
            total_loaded = 0

            for file_path in log_files:
                record = ApplicationLogParser.parse_file(file_path)
                if record:
                    batch.append(record)

                if len(batch) >= batch_size:
                    total_loaded += self.insert_batch(conn, batch)
                    batch.clear()

            if batch:
                total_loaded += self.insert_batch(conn, batch)

            logger.info("Ingestion completed: %d records loaded into MySQL.", total_loaded)
        finally:
            conn.close()


def main() -> None:
    project_root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Ingest logs into MySQL application table.")
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=project_root / "applications",
        help="Path to raw application files.",
    )
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", 3306)))
    parser.add_argument("--user", default=os.getenv("MYSQL_USER", "root"))
    parser.add_argument("--password", default=os.getenv("MYSQL_PASSWORD", ""))
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "inato"))
    parser.add_argument("--batch-size", type=int, default=500)

    args = parser.parse_args()

    db_config = {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "database": args.database,
    }

    loader = MySQLApplicationLoader(db_config=db_config)
    loader.run_pipeline(source_dir=args.source_dir, batch_size=args.batch_size)


if __name__ == "__main__":
    main()