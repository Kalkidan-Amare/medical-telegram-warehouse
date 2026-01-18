import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import Column, DateTime, Float, Integer, MetaData, String, Table, create_engine


def ensure_table(metadata: MetaData) -> Table:
    return Table(
        "image_detections",
        metadata,
        Column("message_id", Integer, nullable=False),
        Column("image_path", String, nullable=False),
        Column("detected_classes", String),
        Column("confidence_score", Float),
        Column("image_category", String),
        Column("loaded_at", DateTime, nullable=False, default=datetime.utcnow),
        schema="raw",
    )


def main() -> None:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not set")

    csv_path = Path(os.getenv("YOLO_OUTPUT_CSV", "data/processed/yolo_detections.csv"))
    if not csv_path.exists():
        print("YOLO output CSV not found. Run src/yolo_detect.py first.")
        return

    engine = create_engine(database_url)
    metadata = MetaData()
    table = ensure_table(metadata)

    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS raw")
        metadata.create_all(connection)

        df = pd.read_csv(csv_path)
        df["loaded_at"] = datetime.utcnow()
        df.to_sql(
            table.name,
            connection,
            schema=table.schema,
            if_exists="append",
            index=False,
            method="multi",
        )

    print(f"Loaded {len(df)} detections into raw.image_detections")


if __name__ == "__main__":
    main()
