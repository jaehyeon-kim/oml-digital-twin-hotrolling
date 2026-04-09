from src.config import (
    TOPIC_GROUND_TRUTH,
    TOPIC_LIFECYCLE,
    TOPIC_PREDICTION_REQUESTS,
    TOPIC_TELEMETRY,
)


def custom_topic_router(data: dict) -> str:
    """Routes the dynamic-des streams to specific Kafka topics."""
    stream_type = data.get("stream_type")

    if stream_type == "telemetry":
        return TOPIC_TELEMETRY

    value = data.get("value", {})
    if isinstance(value, dict):
        event_type = value.get("event_type")
        if event_type == "prediction_request":
            return TOPIC_PREDICTION_REQUESTS
        elif event_type == "ground_truth":
            return TOPIC_GROUND_TRUTH

    return TOPIC_LIFECYCLE
