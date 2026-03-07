import json
import logging
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import (
    AfterWatermark,
    AfterProcessingTime,
    AccumulationMode,
)

from backend.pipelines.feedback_processor import (
    ParseFeedbackFn,
    FilterBySessionFn,
    ScoreFeedbackFn,
    UpdateFirestoreFn,
)

logger = logging.getLogger(__name__)


def _require_env(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise EnvironmentError(f"Required environment variable '{key}' is not set.")
    return val



class ParseEventFn(beam.DoFn):
    VALID_ACTIONS = {"play", "skip", "complete", "like", "dislike", "replay"}

    def process(self, element):
        try:
            event = json.loads(element.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Failed to decode message: {e}")
            return

        missing = {"room_id", "song_id"} - event.keys()
        if missing:
            logger.warning(f"Event missing required fields {missing}: {event}")
            return

        action = event.get("event_type") or event.get("action", "")
        if action not in self.VALID_ACTIONS:
            logger.warning(f"Unknown action '{action}' in event: {event}")
            return

        event["action"] = action
        yield event


class AggregateMetricsFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        (room_id, song_id), events = element
        events = list(events)

        play_count     = sum(1 for e in events if e["action"] == "play")
        skip_count     = sum(1 for e in events if e["action"] == "skip")
        complete_count = sum(1 for e in events if e["action"] == "complete")
        total          = play_count + skip_count + complete_count
        completion_rate = complete_count / total if total > 0 else 0.0

        song_name = next(
            (e.get("song_name") for e in events if e.get("song_name", "Unknown") != "Unknown"),
            "Unknown"
        )
        artist = next(
            (e.get("artist") for e in events if e.get("artist", "Unknown") != "Unknown"),
            "Unknown"
        )

        yield {
            "room_id":         room_id,
            "song_id":         song_id,
            "song_name":       song_name,
            "artist":          artist,
            "play_count":      play_count,
            "skip_count":      skip_count,
            "complete_count":  complete_count,
            "completion_rate": round(completion_rate, 4),
            "total_events":    total,
            "window_start":    window.start.to_utc_datetime().isoformat(),
            "window_end":      window.end.to_utc_datetime().isoformat(),
        }


class FormatForGCSFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        record = dict(element)
        record["_window_start"] = window.start.to_utc_datetime().isoformat()
        record["_window_end"]   = window.end.to_utc_datetime().isoformat()
        yield json.dumps(record)


def build_options() -> PipelineOptions:
    options = PipelineOptions()
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming      = True
    options.view_as(StandardOptions).runner         = "DataflowRunner"

    gcp = options.view_as(GoogleCloudOptions)
    gcp.project          = _require_env("GCP_PROJECT_ID")
    gcp.region           = _require_env("GCP_REGION")
    gcp.temp_location    = _require_env("GCS_TEMP_LOCATION")
    gcp.staging_location = _require_env("GCS_STAGING_LOCATION")
    gcp.job_name         = f"auxless-streaming-{os.environ.get('SESSION_ID', 'global')}"

    return options


def run():
    project_id   = _require_env("GCP_PROJECT_ID")
    pubsub_topic = _require_env("PUBSUB_TOPIC")
    gcs_raw_path = _require_env("GCS_RAW_EVENTS_PATH")
    bq_table     = _require_env("BQ_TABLE")
    db_id        = _require_env("FIRESTORE_DATABASE")
    window_size  = int(os.environ.get("WINDOW_SIZE_SECONDS", "60"))

    bq_schema = (
        "room_id:STRING,song_id:STRING,song_name:STRING,artist:STRING,"
        "play_count:INTEGER,skip_count:INTEGER,complete_count:INTEGER,"
        "completion_rate:FLOAT,total_events:INTEGER,"
        "window_start:STRING,window_end:STRING"
    )

    options = build_options()

    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            | "Read PubSub"  >> beam.io.ReadFromPubSub(topic=pubsub_topic)
            | "Parse Events" >> beam.ParDo(ParseEventFn())
        )
        windowed = (
            parsed
            | "Window" >> beam.WindowInto(
                FixedWindows(window_size),
                trigger=AfterWatermark(late=AfterProcessingTime(delay=30)),
                accumulation_mode=AccumulationMode.ACCUMULATING,
                allowed_lateness=30
            )
        )
        (
            windowed
            | "Format GCS" >> beam.ParDo(FormatForGCSFn())
            | "Write GCS"  >> beam.io.WriteToText(
                gcs_raw_path,
                file_name_suffix=".jsonl",
                num_shards=1
            )
        )
        (
            windowed
            | "Key for BQ"   >> beam.Map(lambda x: ((x["room_id"], x["song_id"]), x))
            | "Group for BQ" >> beam.GroupByKey()
            | "Aggregate"    >> beam.ParDo(AggregateMetricsFn())
            | "Write BQ"     >> beam.io.WriteToBigQuery(
                bq_table,
                schema=bq_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
        (
            windowed
            | "Score Feedback"   >> beam.ParDo(ScoreFeedbackFn())
            | "Update Firestore" >> beam.ParDo(
                UpdateFirestoreFn(
                    project_id=project_id,
                    database_id=db_id
                )
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
