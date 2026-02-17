import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
import json
import logging

PROJECT_ID = "auxless-streaming"
PUBSUB_TOPIC = f"projects/{PROJECT_ID}/topics/auxless_pubsub_topic"

class ParseEvent(beam.DoFn):
    def process(self, element):
        try:
            event = json.loads(element.decode('utf-8'))
            if 'room_id' in event and 'song_id' in event:
                yield event
        except:
            pass

class AggregateMetrics(beam.DoFn):
    def process(self, element):
        (room_id, song_id), events = element
        events = list(events)
        
        play_count = sum(1 for e in events if e.get('event_type') == 'play')
        skip_count = sum(1 for e in events if e.get('event_type') == 'skip')
        complete_count = sum(1 for e in events if e.get('event_type') == 'complete')
        
        total = play_count + skip_count + complete_count
        completion_rate = complete_count / total if total > 0 else 0
        
        yield {
            'room_id': room_id,
            'song_id': song_id,
            'song_name': events[0].get('song_name', 'Unknown'),
            'artist': events[0].get('artist', 'Unknown'),
            'play_count': play_count,
            'skip_count': skip_count,
            'complete_count': complete_count,
            'completion_rate': round(completion_rate, 2),
            'total_events': len(events)
        }

def run():
    options = PipelineOptions(
        streaming=True,
        project=PROJECT_ID,
        region='us-central1',
        temp_location='gs://auxless-streaming-temp/temp',
        staging_location='gs://auxless-streaming-temp/staging'
    )
    
    schema = 'room_id:STRING,song_id:STRING,song_name:STRING,artist:STRING,play_count:INTEGER,skip_count:INTEGER,complete_count:INTEGER,completion_rate:FLOAT,total_events:INTEGER'
    
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read' >> beam.io.ReadFromPubSub(topic=PUBSUB_TOPIC)
            | 'Parse' >> beam.ParDo(ParseEvent())
            | 'Window' >> beam.WindowInto(FixedWindows(60))
            | 'Key' >> beam.Map(lambda x: ((x['room_id'], x['song_id']), x))
            | 'Group' >> beam.GroupByKey()
            | 'Aggregate' >> beam.ParDo(AggregateMetrics())
            | 'WriteBQ' >> beam.io.WriteToBigQuery(
                f'{PROJECT_ID}:auxless.song_analytics',
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
