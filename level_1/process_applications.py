"""
Application Log Transformation Service (Step 1).

Parses raw delimited application event log files and serializes them
into structured JSON documents adhering to domain specifications.
"""

import argparse
import ast
import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Site:
    """Represents the research site details."""
    name: str
    category: str


@dataclass(frozen=True)
class ApplicationRecord:
    """Represents a validated and normalized trial application."""
    id: str
    therapeutic_area: str
    created_at: str
    site: Site

    def to_dict(self) -> Dict[str, Any]:
        """Convert the record to a dictionary payload."""
        return asdict(self)


class ApplicationParser:
    """Handles raw line parsing and schema transformation."""

    DELIMITER = "|"
    KEY_VALUE_SEPARATOR = "="

    @staticmethod
    def _parse_site_dict(site_payload: str) -> Site:
        """Safely parse literal dictionary and normalize keys."""
        try:
            site_obj = ast.literal_eval(site_payload.strip())
            if not isinstance(site_obj, dict):
                raise ValueError("Parsed site payload is not a valid dictionary mapping.")
        except (ValueError, SyntaxError) as exc:
            raise ValueError(f"Failed to parse site field: {site_payload}") from exc

        # Handle variations between 'name'/'site_name' and 'category'/'site_category'
        site_name = site_obj.get("name") or site_obj.get("site_name")
        site_category = site_obj.get("category") or site_obj.get("site_category")

        if not site_name or not site_category:
            raise ValueError(f"Missing required site attributes in payload: {site_obj}")

        return Site(name=str(site_name), category=str(site_category))

    @classmethod
    def parse_raw_log(cls, raw_text: str) -> Optional[ApplicationRecord]:
        """
        Parses pipe-delimited raw log text into a structured ApplicationRecord.
        """
        raw_text = raw_text.strip()
        if not raw_text:
            return None

        fields: Dict[str, str] = {}
        tokens = raw_text.split(cls.DELIMITER)

        for token in tokens:
            token = token.strip()
            if not token:
                continue
            if cls.KEY_VALUE_SEPARATOR not in token:
                logger.warning("Skipping malformed token without '=': %s", token)
                continue

            k, v = token.split(cls.KEY_VALUE_SEPARATOR, 1)
            fields[k.strip()] = v.strip()

        app_id = fields.get("id")
        therapeutic_area = fields.get("therapeutic_area")
        created_at = fields.get("created_at")
        site_raw = fields.get("site")

        if not all([app_id, therapeutic_area, created_at, site_raw]):
            logger.warning("Skipping record due to missing required fields: %s", fields)
            return None

        site = cls._parse_site_dict(site_raw)
        return ApplicationRecord(
            id=app_id,
            therapeutic_area=therapeutic_area,
            created_at=created_at,
            site=site,
        )


class ApplicationLogPipeline:
    """Pipeline orchestrator for reading, transforming, and persisting records."""

    def __init__(self, source_directory: Path, target_directory: Path) -> None:
        self.source_directory = source_directory
        self.target_directory = target_directory

    def run(self) -> None:
        """Executes the transformation workflow."""
        if not self.source_directory.is_dir():
            raise FileNotFoundError(f"Source directory does not exist: {self.source_directory}")

        self.target_directory.mkdir(parents=True, exist_ok=True)
        source_files = list(self.source_directory.glob("*.json"))

        logger.info("Starting ingestion from %s (%d files detected)", self.source_directory, len(source_files))

        success_count = 0
        failure_count = 0

        for file_path in source_files:
            try:
                raw_content = file_path.read_text(encoding="utf-8")
                record = ApplicationParser.parse_raw_log(raw_content)

                if record is None:
                    failure_count += 1
                    continue

                output_path = self.target_directory / f"application-{record.id}.json"
                with open(output_path, mode="w", encoding="utf-8") as out_file:
                    json.dump(record.to_dict(), out_file, indent=4)

                success_count += 1
            except Exception as exc:
                logger.error("Error processing file %s: %s", file_path.name, exc)
                failure_count += 1

        logger.info(
            "Transformation job completed. Processed: %d, Failed: %d. Output saved to %s",
            success_count,
            failure_count,
            self.target_directory,
        )


def build_parser() -> argparse.ArgumentParser:
    project_root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Application log transformation pipeline.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=project_root / "applications",
        help="Directory containing the raw application log files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=project_root / "processed",
        help="Directory where transformed JSON files should be stored.",
    )
    return parser


if __name__ == "__main__":
    cli_parser = build_parser()
    args = cli_parser.parse_args()

    pipeline = ApplicationLogPipeline(
        source_directory=args.input_dir,
        target_directory=args.output_dir,
    )
    pipeline.run()