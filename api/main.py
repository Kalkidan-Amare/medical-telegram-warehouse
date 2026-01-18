from typing import List

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import text

from api.database import get_db
from api.schemas import (
    ChannelActivityResponse,
    ChannelDailyActivity,
    MessageSearchResult,
    TopProduct,
    VisualContentStat,
)

app = FastAPI(
    title="Medical Telegram Warehouse API",
    description="Analytical API for Telegram medical business insights.",
    version="1.0.0",
)


@app.get(
    "/api/reports/top-products",
    response_model=List[TopProduct],
    summary="Most mentioned products",
    description="Returns the most frequently mentioned terms across all channels.",
)
def get_top_products(limit: int = Query(10, ge=1, le=100)) -> List[TopProduct]:
    query = text(
        """
        with tokens as (
            select
                lower(regexp_replace(token, '[^a-z0-9]+', '', 'g')) as term
            from marts.fct_messages,
            lateral regexp_split_to_table(coalesce(message_text, ''), '\\s+') as token
        )
        select term, count(*) as mention_count
        from tokens
        where term <> '' and length(term) >= 3
        group by term
        order by mention_count desc
        limit :limit
        """
    )

    with get_db() as db:
        rows = db.execute(query, {"limit": limit}).mappings().all()
        return [TopProduct(**row) for row in rows]


@app.get(
    "/api/channels/{channel_name}/activity",
    response_model=ChannelActivityResponse,
    summary="Channel activity",
    description="Returns daily posting activity for a specific channel.",
)
def get_channel_activity(channel_name: str) -> ChannelActivityResponse:
    daily_query = text(
        """
        select
            d.full_date::text as date,
            count(*) as message_count
        from marts.fct_messages f
        join marts.dim_channels c on f.channel_key = c.channel_key
        join marts.dim_dates d on f.date_key = d.date_key
        where c.channel_name = :channel_name
        group by d.full_date
        order by d.full_date
        """
    )

    with get_db() as db:
        rows = db.execute(daily_query, {"channel_name": channel_name}).mappings().all()
        if not rows:
            raise HTTPException(status_code=404, detail="Channel not found")

    daily_activity = [ChannelDailyActivity(**row) for row in rows]
    total_messages = sum(item.message_count for item in daily_activity)
    return ChannelActivityResponse(
        channel_name=channel_name,
        total_messages=total_messages,
        daily_activity=daily_activity,
    )


@app.get(
    "/api/search/messages",
    response_model=List[MessageSearchResult],
    summary="Message search",
    description="Searches for messages containing a keyword.",
)
def search_messages(
    query: str = Query(..., min_length=2),
    limit: int = Query(20, ge=1, le=100),
) -> List[MessageSearchResult]:
    sql = text(
        """
        select
            f.message_id,
            c.channel_name,
            d.full_date::text as message_date,
            f.message_text
        from marts.fct_messages f
        join marts.dim_channels c on f.channel_key = c.channel_key
        join marts.dim_dates d on f.date_key = d.date_key
        where f.message_text ilike :pattern
        order by d.full_date desc
        limit :limit
        """
    )

    with get_db() as db:
        rows = db.execute(
            sql,
            {"pattern": f"%{query}%", "limit": limit},
        ).mappings().all()

    return [MessageSearchResult(**row) for row in rows]


@app.get(
    "/api/reports/visual-content",
    response_model=List[VisualContentStat],
    summary="Visual content statistics",
    description="Returns image usage stats per channel.",
)
def get_visual_content_stats() -> List[VisualContentStat]:
    sql = text(
        """
        with counts as (
            select
                c.channel_name,
                d.image_category,
                count(*) as image_count
            from marts.fct_image_detections d
            join marts.dim_channels c on d.channel_key = c.channel_key
            group by c.channel_name, d.image_category
        ),
        totals as (
            select channel_name, sum(image_count) as total_images
            from counts
            group by channel_name
        )
        select
            counts.channel_name,
            counts.image_category,
            counts.image_count,
            round((counts.image_count::float / totals.total_images) * 100, 2) as percentage
        from counts
        join totals on counts.channel_name = totals.channel_name
        order by counts.channel_name, counts.image_category
        """
    )

    with get_db() as db:
        rows = db.execute(sql).mappings().all()

    return [VisualContentStat(**row) for row in rows]
