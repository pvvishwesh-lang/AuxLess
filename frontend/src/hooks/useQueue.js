import { useState, useEffect } from 'react';
import {
  collection, onSnapshot,
  query, orderBy
} from 'firebase/firestore';
import { mlDb } from '../config/firebase';

const toSessionId = (id) => (id || '').replace(/^AUX-/i, '').toLowerCase();

export function useQueue(roomId) {
  const [queue,   setQueue]   = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!roomId) return;
    const sessionId = toSessionId(roomId);

    // Read from recommendations — where ML writes
    const q = query(
      collection(mlDb, 'sessions', sessionId, 'recommendations'),
      orderBy('rank', 'asc')
    );

    const unsub = onSnapshot(q, (snap) => {
      const tracks = snap.docs.map(d => ({
        id:       d.id,
        ...d.data(),
        title:    d.data().track_title  || d.data().song_name  || d.data().title  || 'Unknown',
        artist:   d.data().artist_name  || d.data().artist     || '',
        likes:    d.data().like_count   || 0,
        dislikes: d.data().dislike_count || 0,
        score:    d.data().final_score  || d.data().score      || 0,
        video_id: d.data().video_id     || d.id,
        genre:    d.data().genre        || '',
        image:    d.data().artwork_url  || d.data().image_url  || null,
      }));
      setQueue(tracks);
      setLoading(false);
    }, () => setLoading(false));

    return () => unsub();
  }, [roomId]);

  return { queue, loading };
}