import { useState, useEffect } from 'react';
import {
  collection, onSnapshot,
  query, orderBy
} from 'firebase/firestore';
import { db } from '../config/firebase';

// Listens to sessions/{roomId}/queue
// ML model reorders this after each feedback window
export function useQueue(roomId) {
  const [queue,   setQueue]   = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!roomId) return;

    // order by score desc — matches ScoreFeedbackFn output
    const q = query(
      collection(db, 'sessions', roomId, 'queue'),
      orderBy('score', 'desc')
    );

    const unsub = onSnapshot(q, (snap) => {
      const tracks = snap.docs.map(d => ({
        id:        d.id,
        ...d.data(),
        // map pipeline field names → UI field names
        title:     d.data().song_name  || d.data().title,
        artist:    d.data().artist_name|| d.data().artist,
        likes:     d.data().like_count || 0,
        dislikes:  d.data().dislike_count || 0,
        score:     d.data().score      || 0,
      }));
      setQueue(tracks);
      setLoading(false);
    });

    return () => unsub();
  }, [roomId]);

  return { queue, loading };
}