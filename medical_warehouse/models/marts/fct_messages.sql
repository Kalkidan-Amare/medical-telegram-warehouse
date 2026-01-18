with messages as (
    select * from {{ ref('stg_telegram_messages') }}
),

channels as (
    select * from {{ ref('dim_channels') }}
),

dates as (
    select * from {{ ref('dim_dates') }}
)

select
    messages.message_id,
    channels.channel_key,
    dates.date_key,
    messages.message_text,
    messages.message_length,
    messages.view_count,
    messages.forward_count,
    messages.has_image
from messages
left join channels
    on messages.channel_name = channels.channel_name
left join dates
    on messages.message_date::date = dates.full_date
