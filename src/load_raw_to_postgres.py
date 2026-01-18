import json
import os
from datetime import datetime
from pathlib import Path
from typing import Iterable, List

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import JSON, Boolean, Column, DateTime, Integer, MetaData, String, Table, Text, create_engine


def collect_json_files(base_path: Path) -> List[Path]:
    return list(base_path.rglob("*.json"))


def load_messages(file_path: Path) -> List[dict]:
    with file_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def ensure_table(metadata: MetaData) -> Table:
    return Table(
        "telegram_messages",
        metadata,
        Column("message_id", Integer, nullable=False),
        Column("channel_name", String, nullable=False),
        Column("message_date", DateTime, nullable=False),
        Column("message_text", Text),
        Column("has_media", Boolean, nullable=False),
        Column("image_path", Text),
        Column("views", Integer),
        Column("forwards", Integer),
        Column("raw_json", JSON, nullable=False),
        Column("loaded_at", DateTime, nullable=False, default=datetime.utcnow),
        schema="raw",
    )


def normalize_records(records: Iterable[dict]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    df["message_date"] = pd.to_datetime(df["message_date"], errors="coerce")
    df["raw_json"] = records
    df["loaded_at"] = datetime.utcnow()
    return df


def main() -> None:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not set")

    base_path = Path(os.getenv("DATA_LAKE_PATH", "data/raw")) / "telegram_messages"
    json_files = collect_json_files(base_path)

    if not json_files:
        print("No JSON files found in data lake.")
        return

    engine = create_engine(database_url)
    metadata = MetaData()
    table = ensure_table(metadata)

    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS raw")
        metadata.create_all(connection)

        for json_file in json_files:
            records = load_messages(json_file)
            if not records:
                continue
            df = normalize_records(records)
            df.to_sql(
                table.name,
                connection,
                schema=table.schema,
                if_exists="append",
                index=False,
                method="multi",
            )
            print(f"Loaded {len(df)} records from {json_file}")


if __name__ == "__main__":
    main()
