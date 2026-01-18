from typing import List, Optional

from pydantic import BaseModel, Field


class TopProduct(BaseModel):
    term: str
    mention_count: int


class ChannelDailyActivity(BaseModel):
    date: str
    message_count: int


class ChannelActivityResponse(BaseModel):
    channel_name: str
    total_messages: int
    daily_activity: List[ChannelDailyActivity]


class MessageSearchResult(BaseModel):
    message_id: int
    channel_name: str
    message_date: str
    message_text: Optional[str]


class VisualContentStat(BaseModel):
    channel_name: str
    image_category: str
    image_count: int
    percentage: float = Field(..., ge=0, le=100)
