import json
from google.cloud import pubsub_v1
from datetime import datetime
import functions_framework

PROJECT_ID = "auxless-streaming"
TOPIC_NAME = "auxless_pubsub_topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

@functions_framework.http
def publish_music_event(request):
    """Publish music playback events to Pub/Sub"""
    
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
        }
        return ('', 204, headers)
    
    headers = {'Access-Control-Allow-Origin': '*'}
    
    try:
        request_json = request.get_json(silent=True)
        
        if not request_json:
            return ({'error': 'No JSON body'}, 400, headers)
        
        required = ['room_id', 'user_id', 'song_id', 'event_type']
        missing = [f for f in required if f not in request_json]
        
        if missing:
            return ({'error': f'Missing: {", ".join(missing)}'}, 400, headers)
        
        event = {
            'room_id': request_json['room_id'],
            'user_id': request_json['user_id'],
            'song_id': request_json['song_id'],
            'song_name': request_json.get('song_name', 'Unknown'),
            'artist': request_json.get('artist', 'Unknown'),
            'event_type': request_json['event_type'],
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        
        message_json = json.dumps(event)
        message_bytes = message_json.encode('utf-8')
        
        future = publisher.publish(topic_path, message_bytes)
        message_id = future.result()
        
        return ({
            'status': 'success',
            'message_id': message_id,
            'event': event
        }, 200, headers)
        
    except Exception as e:
        return ({'error': str(e)}, 500, headers)
