import argparse
import asyncio
import json
import logging
import os
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import Message


@dataclass
class TelegramMessageRecord:
    message_id: int
    channel_name: str
    message_date: str
    message_text: Optional[str]
    has_media: bool
    image_path: Optional[str]
    views: Optional[int]
    forwards: Optional[int]


def slugify(value: str) -> str:
    return "".join(char.lower() if char.isalnum() else "_" for char in value).strip("_")


def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_path), logging.StreamHandler()],
    )


def parse_channels(channels_raw: str) -> List[str]:
    channels = [c.strip() for c in channels_raw.split(",") if c.strip()]
    return channels


def build_message_record(
    message: Message,
    channel_name: str,
    image_path: Optional[Path],
) -> TelegramMessageRecord:
    return TelegramMessageRecord(
        message_id=message.id,
        channel_name=channel_name,
        message_date=message.date.isoformat(),
        message_text=message.message,
        has_media=bool(message.media),
        image_path=str(image_path) if image_path else None,
        views=message.views,
        forwards=message.forwards,
    )


def get_partition_path(base_path: Path, message_date: datetime, channel_name: str) -> Path:
    date_partition = message_date.date().isoformat()
    channel_slug = slugify(channel_name)
    return base_path / "telegram_messages" / date_partition / f"{channel_slug}.json"


async def scrape_channel(
    client: TelegramClient,
    channel: str,
    raw_path: Path,
    images_path: Path,
    limit: Optional[int],
    since: Optional[datetime],
    until: Optional[datetime],
) -> None:
    logging.info("Scraping channel: %s", channel)
    channel_slug = slugify(channel)
    image_dir = images_path / channel_slug
    image_dir.mkdir(parents=True, exist_ok=True)

    records_by_day: Dict[Path, List[TelegramMessageRecord]] = defaultdict(list)

    async for message in client.iter_messages(channel, limit=limit):
        if not message.date:
            continue
        if since and message.date < since:
            continue
        if until and message.date > until:
            continue

        image_path = None
        if message.photo:
            image_path = image_dir / f"{message.id}.jpg"
            await message.download_media(file=str(image_path))

        record = build_message_record(message, channel, image_path)
        partition_path = get_partition_path(raw_path, message.date, channel)
        records_by_day[partition_path].append(record)

    for partition_path, records in records_by_day.items():
        partition_path.parent.mkdir(parents=True, exist_ok=True)
        payload = [asdict(record) for record in records]
        with partition_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
        logging.info("Wrote %s records to %s", len(records), partition_path)


async def run_scraper(args: argparse.Namespace) -> None:
    load_dotenv()

    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    session_name = os.getenv("TELEGRAM_SESSION_NAME", "medical_warehouse")
    channels_raw = os.getenv(
        "TELEGRAM_CHANNELS", "chemed,lobelia4cosmetics,tikvahpharma"
    )

    if not api_id or not api_hash:
        raise ValueError("Missing TELEGRAM_API_ID or TELEGRAM_API_HASH in environment")

    channels = parse_channels(channels_raw)
    raw_path = Path(os.getenv("DATA_LAKE_PATH", "data/raw"))
    images_path = Path(os.getenv("IMAGES_PATH", "data/raw/images"))
    log_path = Path("logs") / "scraper.log"
    setup_logging(log_path)

    since = datetime.fromisoformat(args.since) if args.since else None
    until = datetime.fromisoformat(args.until) if args.until else None

    async with TelegramClient(session_name, int(api_id), api_hash) as client:
        for channel in channels:
            await scrape_channel(
                client=client,
                channel=channel,
                raw_path=raw_path,
                images_path=images_path,
                limit=args.limit,
                since=since,
                until=until,
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telegram channel scraper")
    parser.add_argument("--limit", type=int, default=None, help="Limit messages per channel")
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="ISO datetime filter for earliest message (e.g. 2024-01-01T00:00:00)",
    )
    parser.add_argument(
        "--until",
        type=str,
        default=None,
        help="ISO datetime filter for latest message (e.g. 2024-01-31T23:59:59)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    asyncio.run(run_scraper(arguments))
