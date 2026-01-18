with source as (
    select *
    from raw.telegram_messages
)

select
    cast(message_id as bigint) as message_id,
    lower(channel_name) as channel_name,
    cast(message_date as timestamp) as message_date,
    nullif(message_text, '') as message_text,
    cast(has_media as boolean) as has_media,
    image_path,
    cast(views as integer) as view_count,
    cast(forwards as integer) as forward_count,
    length(coalesce(message_text, '')) as message_length,
    case
        when image_path is not null then true
        else false
    end as has_image
from source
where message_id is not null
