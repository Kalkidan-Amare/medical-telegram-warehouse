with detections as (
    select
        message_id,
        image_path,
        detected_classes,
        confidence_score,
        image_category
    from raw.image_detections
),

messages as (
    select * from {{ ref('fct_messages') }}
)

select
    detections.message_id,
    messages.channel_key,
    messages.date_key,
    detections.detected_classes,
    detections.confidence_score,
    detections.image_category
from detections
left join messages
    on detections.message_id = messages.message_id
