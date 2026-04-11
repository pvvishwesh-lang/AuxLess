export const GENRES = [
  'Hip-Hop','Pop','R&B','Electronic','Rock',
  'Latin','Afrobeats','Indie','K-Pop','House','Drill','Reggaeton','Jazz','Soul'
];

export const ARTISTS = [
  'Drake','Taylor Swift','Bad Bunny','The Weeknd','Doja Cat',
  'Travis Scott','Billie Eilish','Kendrick Lamar','Sabrina Carpenter',
  'SZA','Post Malone','Olivia Rodrigo','Metro Boomin','Tyler the Creator'
];

export const ARTIST_IMAGES = {
  'Drake':             'https://i.pravatar.cc/80?img=11',
  'Taylor Swift':      'https://i.pravatar.cc/80?img=47',
  'Bad Bunny':         'https://i.pravatar.cc/80?img=12',
  'The Weeknd':        'https://i.pravatar.cc/80?img=13',
  'Doja Cat':          'https://i.pravatar.cc/80?img=44',
  'Travis Scott':      'https://i.pravatar.cc/80?img=15',
  'Billie Eilish':     'https://i.pravatar.cc/80?img=46',
  'Kendrick Lamar':    'https://i.pravatar.cc/80?img=17',
  'Sabrina Carpenter': 'https://i.pravatar.cc/80?img=48',
  'SZA':               'https://i.pravatar.cc/80?img=45',
  'Post Malone':       'https://i.pravatar.cc/80?img=20',
  'Olivia Rodrigo':    'https://i.pravatar.cc/80?img=49',
  'Metro Boomin':      'https://i.pravatar.cc/80?img=22',
  'Tyler the Creator': 'https://i.pravatar.cc/80?img=23',
};

// Matches AggregateMetricsFn output shape from streaming_pipeline.py
export const MOCK_STREAM_EVENTS = [
  { room_id:'AUX-7749', song_id:'v1', song_name:'Rich Flex',      artist:'Drake & 21 Savage',          play_count:12, skip_count:2, complete_count:8,  completion_rate:0.571, total_events:22, window_start:'2024-01-01T22:00:00', window_end:'2024-01-01T22:01:00' },
  { room_id:'AUX-7749', song_id:'v2', song_name:'Anti-Hero',       artist:'Taylor Swift',               play_count:9,  skip_count:4, complete_count:5,  completion_rate:0.556, total_events:18, window_start:'2024-01-01T22:01:00', window_end:'2024-01-01T22:02:00' },
  { room_id:'AUX-7749', song_id:'v3', song_name:'Ella Baila Sola', artist:'Eslabon Armado',             play_count:11, skip_count:1, complete_count:9,  completion_rate:0.818, total_events:21, window_start:'2024-01-01T22:02:00', window_end:'2024-01-01T22:03:00' },
  { room_id:'AUX-7749', song_id:'v4', song_name:"Creepin'",        artist:'Metro Boomin ft. The Weeknd',play_count:8,  skip_count:3, complete_count:4,  completion_rate:0.500, total_events:15, window_start:'2024-01-01T22:03:00', window_end:'2024-01-01T22:04:00' },
  { room_id:'AUX-7749', song_id:'v5', song_name:'Flowers',         artist:'Miley Cyrus',                play_count:7,  skip_count:5, complete_count:3,  completion_rate:0.429, total_events:15, window_start:'2024-01-01T22:04:00', window_end:'2024-01-01T22:05:00' },
];

// Matches ScoreFeedbackFn weights: like+2, dislike-2, skip-0.5, replay+1.5
export const MOCK_FEEDBACK_SCORES = [
  { video_id:'v1', song_name:'Rich Flex',      score:14.5, like_count:6, dislike_count:1, skip_count:2, replay_count:2 },
  { video_id:'v2', song_name:'Anti-Hero',       score:4.0,  like_count:3, dislike_count:2, skip_count:4, replay_count:1 },
  { video_id:'v3', song_name:'Ella Baila Sola', score:17.0, like_count:7, dislike_count:0, skip_count:1, replay_count:3 },
  { video_id:'v4', song_name:"Creepin'",        score:6.5,  like_count:4, dislike_count:1, skip_count:3, replay_count:1 },
  { video_id:'v5', song_name:'Flowers',         score:-1.5, like_count:1, dislike_count:3, skip_count:5, replay_count:0 },
];

// Matches bias_analyser slices: genre + country
export const MOCK_BIAS_METRICS = {
  genre: [
    { slice:'Hip-Hop',    pct:44, adjusted:false, color:'#1DB954' },
    { slice:'Pop',        pct:28, adjusted:false, color:'#7C3AED' },
    { slice:'Latin',      pct:16, adjusted:true,  color:'#EC4899' },
    { slice:'R&B',        pct:12, adjusted:true,  color:'#F59E0B' },
  ],
  country: [
    { slice:'US',  pct:62, adjusted:true,  color:'#3B82F6' },
    { slice:'MX',  pct:18, adjusted:false, color:'#10B981' },
    { slice:'UK',  pct:12, adjusted:false, color:'#8B5CF6' },
    { slice:'Other',pct:8, adjusted:true,  color:'#F97316' },
  ],
};

// Matches run_for_session pipeline steps
export const PIPELINE_STEPS = [
  { step:'Batch ingest',     detail:'YouTube OAuth + iTunes metadata via Dataflow',           status:'done',    color:'#1DB954' },
  { step:'Schema validation',detail:'Required columns, numeric fields, ratio range checks',   status:'done',    color:'#1DB954' },
  { step:'Bias detection',   detail:'Slicing across genre + country, upsampling/downsampling',status:'done',    color:'#1DB954' },
  { step:'ML clustering',    detail:'Genre grouping, feedback-weighted ranking',               status:'running', color:'#F59E0B' },
  { step:'Streaming pipeline',detail:'Pub/Sub → ParseEventFn → AggregateMetricsFn → BQ + GCS',status:'running', color:'#F59E0B' },
  { step:'Feedback loop',    detail:'ScoreFeedbackFn: like+2, dislike-2, skip-0.5, replay+1.5',status:'live',   color:'#7C3AED' },
  { step:'Firestore update', detail:'UpdateFirestoreFn writes scores + user_feedback docs',    status:'live',   color:'#7C3AED' },
];

export const MOCK_QUEUE = [
  { id:'1', title:'Rich Flex',      artist:'Drake & 21 Savage',           artistImg:'https://i.pravatar.cc/80?img=11', genre:'Hip-Hop', dur:'3:21', likes:4, dislikes:0, playing:true,  score:14.5 },
  { id:'2', title:'Anti-Hero',      artist:'Taylor Swift',                artistImg:'https://i.pravatar.cc/80?img=47', genre:'Pop',     dur:'3:20', likes:3, dislikes:1, playing:false, score:4.0  },
  { id:'3', title:'Ella Baila Sola',artist:'Eslabon Armado',              artistImg:'https://i.pravatar.cc/80?img=12', genre:'Latin',   dur:'3:58', likes:3, dislikes:0, playing:false, score:17.0 },
  { id:'4', title:"Creepin'",       artist:'Metro Boomin ft. The Weeknd', artistImg:'https://i.pravatar.cc/80?img=22', genre:'R&B',     dur:'3:32', likes:2, dislikes:0, playing:false, score:6.5  },
  { id:'5', title:'Flowers',        artist:'Miley Cyrus',                 artistImg:'https://i.pravatar.cc/80?img=49', genre:'Pop',     dur:'3:21', likes:1, dislikes:1, playing:false, score:-1.5 },
];

export const MOCK_MEMBERS = [
  { id:1, name:'Jordan M.', av:'JM', active:true  },
  { id:2, name:'Priya K.',  av:'PK', active:true  },
  { id:3, name:'Alex T.',   av:'AT', active:true  },
  { id:4, name:'Sam R.',    av:'SR', active:false },
];

export const GENRES_DIST = [
  { g:'Hip-Hop', p:44, c:'#1DB954' },
  { g:'Pop',     p:28, c:'#7C3AED' },
  { g:'Latin',   p:16, c:'#EC4899' },
  { g:'R&B',     p:12, c:'#F59E0B' },
];