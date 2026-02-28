import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
)
from apache_beam.io import ReadFromPubSub
import argparse
import json
import logging
import os

from backend.pipelines.api.feedback_processor import (
    ParseFeedbackFn,
    FilterBySessionFn,
    ScoreFeedbackFn,
    UpdateFirestoreFn
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--session_id",         required=True)
    parser.add_argument("--input_subscription",  required=True)
    parser.add_argument("--firestore_project",   required=True)
    parser.add_argument("--firestore_database",  required=True)
    parser.add_argument("--bucket",              required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True 

    gcp_opts = options.view_as(GoogleCloudOptions)
    gcp_opts.project          = known_args.firestore_project
    gcp_opts.region           = "us-central1"
    gcp_opts.staging_location = f"gs://{known_args.bucket}/staging"
    gcp_opts.temp_location    = f"gs://{known_args.bucket}/temp"

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read Feedback"     >> ReadFromPubSub(
                subscription=known_args.input_subscription
            )
            | "Parse"             >> beam.ParDo(ParseFeedbackFn())
            | "Filter Session"    >> beam.ParDo(
                FilterBySessionFn(known_args.session_id)
            )
            | "Score Feedback"    >> beam.ParDo(ScoreFeedbackFn())
            | "Update Firestore"  >> beam.ParDo(
                UpdateFirestoreFn(
                    project_id=known_args.firestore_project,
                    database_id=known_args.firestore_database,
                    session_id=known_args.session_id
                )
            )
        )


if __name__ == "__main__":
    run()
